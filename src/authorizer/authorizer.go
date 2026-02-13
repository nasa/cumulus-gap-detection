package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/MicahParks/keyfunc/v2"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/secretsmanager"
	"github.com/golang-jwt/jwt/v5"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	jwksURLFmt = "https://%s/discovery/keys"
	issuerFmt  = "http://%s/services/trust"
)

var (
	jwks          *keyfunc.JWKS
	jwksOnce      sync.Once
	jwksErr       error
	logger        *zap.Logger
	certCache     *tls.Certificate
	certCacheOnce sync.Once
	certCacheErr  error

	config struct {
		jwksURL         string
		issuer          string
		audience        string
		adminRole       string
		publicRole      string
		validateURL     string
		secretArn       string
		authorizedHosts []string
	}
)

func init() {
	var err error
	config := zap.NewProductionConfig()

	// Default to info, override with LOG_LEVEL env var
	if logLevel := os.Getenv("LOG_LEVEL"); logLevel != "" {
		var level zapcore.Level
		if err := level.UnmarshalText([]byte(logLevel)); err == nil {
			config.Level = zap.NewAtomicLevelAt(level)
		}
	}

	logger, err = config.Build()
	if err != nil {
		panic(err)
	}
}

var configOnce sync.Once

func initConfig() {
	configOnce.Do(func() {
		getEnv := func(k string) string {
			v := os.Getenv(k)
			if v == "" {
				logger.Fatal("INIT: Required variable not found in environment",
					zap.String("required", k))
			}
			return v
		}
		// Set backend config from environment variables
		idpHost := getEnv("IDP_HOST")
		authorizedHosts := os.Getenv("AUTHORIZED_HOSTS")
		config.audience = getEnv("AUDIENCE")
		config.adminRole = getEnv("ADMIN_ROLE")
		config.publicRole = getEnv("PUBLIC_ROLE")
		config.validateURL = getEnv("TOKEN_SERVICE_ENDPOINT")
		config.secretArn = getEnv("SERVICE_ACCOUNT_SECRET_ARN")
		config.jwksURL = fmt.Sprintf(jwksURLFmt, idpHost)
		config.issuer = fmt.Sprintf(issuerFmt, idpHost)

		// Parse IP addresses from whitelist env var
		if authorizedHosts != "" {
			config.authorizedHosts = strings.Split(authorizedHosts, ",")
			for i := range config.authorizedHosts {
				config.authorizedHosts[i] = strings.TrimSpace(config.authorizedHosts[i])
			}
		}
		logger.Debug("INIT: Authorized read-only hosts", zap.Strings("ips", config.authorizedHosts))
	})
}

// Initialize public key cache manager for re-use
func initJWKS() error {
	jwksOnce.Do(func() {
		url := config.jwksURL
		start := time.Now()
		jwks, jwksErr = keyfunc.Get(url, keyfunc.Options{
			RefreshInterval: time.Hour,
		})
		logger.Debug("INIT: JWKS fetch completed",
			zap.Duration("elapsed", time.Since(start)),
			zap.Error(jwksErr))
	})
	return jwksErr
}

// Generate resource policy for a given resource and effect (ie. Allow or Deny)
func generatePolicy(effect, message, userID, role string, event events.APIGatewayCustomAuthorizerRequestTypeRequest) events.APIGatewayCustomAuthorizerResponse {
	logger.Info(message,
		zap.String("effect", effect),
		zap.String("user", userID),
		zap.String("role", role),
		zap.String("source_ip", event.RequestContext.Identity.SourceIP),
		zap.String("method", event.HTTPMethod),
		zap.String("path", event.Path),
	)
	if effect == "Deny" {
		event.MethodArn = "*"
	}
	return events.APIGatewayCustomAuthorizerResponse{
		PrincipalID: "user",
		PolicyDocument: events.APIGatewayCustomAuthorizerPolicy{
			Version: "2012-10-17",
			Statement: []events.IAMPolicyStatement{{
				Action:   []string{"execute-api:Invoke"},
				Effect:   effect,
				Resource: []string{event.MethodArn},
			}},
		},
	}
}

func loadServiceAccountCert() (*tls.Certificate, error) {
	certCacheOnce.Do(func() {
		logger.Debug("SA_CERT: Loading service account certificate from Secrets Manager",
			zap.String("secret_arn", config.secretArn))

		sess := session.Must(session.NewSession())
		svc := secretsmanager.New(sess)

		result, err := svc.GetSecretValue(&secretsmanager.GetSecretValueInput{
			SecretId: aws.String(config.secretArn),
		})
		if err != nil {
			certCacheErr = fmt.Errorf("failed to get secret: %w", err)
			return
		}

		logger.Debug("SA_CERT: Successfully retrieved secret from Secrets Manager",
			zap.Int("secret_length", len(*result.SecretString)))

		var secret struct {
			CertPEM string `json:"cert_pem"`
			KeyPEM  string `json:"key_pem"`
		}
		if err := json.Unmarshal([]byte(*result.SecretString), &secret); err != nil {
			certCacheErr = fmt.Errorf("failed to unmarshal secret: %w", err)
			return
		}

		cert, err := tls.X509KeyPair([]byte(secret.CertPEM), []byte(secret.KeyPEM))
		if err != nil {
			certCacheErr = fmt.Errorf("failed to parse X509 key pair: %w", err)
			return
		}

		certCache = &cert
	})
	return certCache, certCacheErr
}

func validateServiceAccountToken(token string) bool {
	logger.Debug("SM_VALIDATE: Starting service account token validation",
		zap.String("token_prefix", token[:min(10, len(token))]),
		zap.Int("token_length", len(token)))
	cert, err := loadServiceAccountCert()
	if err != nil {
		logger.Error("Failed to load service account cert", zap.Error(err))
	} else {
		logger.Debug("SM_VALIDATE: Certificate loaded successfully, creating HTTP client")

		client := &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					Certificates: []tls.Certificate{*cert},
				},
			},
		}

		reqBody, _ := json.Marshal(map[string]string{"token": token})
		logger.Debug("SM_VALIDATE: Sending validation request",
			zap.String("url", config.validateURL),
			zap.Int("request_body_length", len(reqBody)))

		resp, err := client.Post(config.validateURL, "application/json", bytes.NewReader(reqBody))
		if err != nil {
			logger.Error("Service account validation request failed", zap.Error(err))
		} else {
			defer resp.Body.Close()
			logger.Debug("SM_VALIDATE: Received response",
				zap.Int("status_code", resp.StatusCode),
				zap.String("status", resp.Status))

			body, _ := io.ReadAll(resp.Body)
			logger.Debug("SM_VALIDATE: Response body received",
				zap.Int("body_length", len(body)),
				zap.String("body", string(body)))

			if resp.StatusCode != 200 {
				logger.Info("Service account validation rejected", zap.Int("status", resp.StatusCode))
			} else {
				var result struct {
					Status string `json:"status"`
				}

				if err := json.Unmarshal(body, &result); err != nil {
					logger.Error("Failed to parse validation response", zap.Error(err))
				} else {
					logger.Info("SA_VALIDATE: Validation completed",
						zap.String("status", result.Status))
					return result.Status == "success"
				}
			}
		}
	}
	return false
}

func parseToken(authHeader string) (role, userID string) {
	// Extract bearer token
	token := strings.TrimPrefix(authHeader, "Bearer ")
	if token == "" || token == authHeader {
		logger.Debug("Malformed Authorization header")
		return "", ""
	}

	n := len(token)
	if n > 3 && token[:3] == "eyJ" && token[n-1] != '=' {
		// Initialize JWKS
		if err := initJWKS(); err != nil {
			logger.Error("JWKS initialization failed",
				zap.Error(err),
				zap.String("jwks_url", config.jwksURL),
			)
		} else {
			// Validate token: signature, algorithm, issuer, audience
			parsed, err := jwt.Parse(
				token,
				jwks.Keyfunc,
				jwt.WithValidMethods([]string{"RS256"}),
				jwt.WithIssuer(config.issuer),
				jwt.WithAudience(config.audience),
			)
			logger.Debug("Token details", zap.Any("claims", parsed.Claims))
			if err == nil && parsed.Valid {
				claims, _ := parsed.Claims.(jwt.MapClaims)
				role, _ = claims["groups"].(string)
				userID, _ = claims["AgencyUID"].(string)
				return role, userID
			}
			logger.Info("Invalid JWT", zap.Error(err))
		}
	} else {

		logger.Debug("Detected SM token")
		if validateServiceAccountToken(token) {
			return config.publicRole, "Service Account"
		}
	}

	return "", ""
}

func Handler(ctx context.Context, event events.APIGatewayCustomAuthorizerRequestTypeRequest) (events.APIGatewayCustomAuthorizerResponse, error) {
	initConfig()
	var role, userID string
	sourceIP := event.RequestContext.Identity.SourceIP
	authHeader := event.Headers["Authorization"]

	logger.Info("Recieved request",
		zap.String("source_ip", sourceIP),
		zap.String("method", event.HTTPMethod),
		zap.String("path", event.Path),
		zap.String("auth_source", func() string {
			if authHeader != "" {
				return "auth header"
			}
			return "ip address"
		}()),
	)

	// Assign role from token if auth header is set
	if authHeader != "" {
		role, userID = parseToken(authHeader)
		// Assign public role if source IP is whitelisted
	} else if slices.Contains(config.authorizedHosts, sourceIP) {
		role = config.publicRole
		userID = sourceIP
	}

	// Allow all admin requests
	if role == config.adminRole {
		return generatePolicy("Allow", "Authorizing admin request", userID, role, event), nil
	}

	// Allow public requests for GET
	if event.HTTPMethod == "GET" && role == config.publicRole {
		return generatePolicy("Allow", "Authorizing public request", userID, role, event), nil
	}

	// Default deny catch-all
	return generatePolicy("Deny", "Unauthorized request", userID, role, event), nil
}

func main() {
	defer logger.Sync()
	lambda.Start(Handler)
}
