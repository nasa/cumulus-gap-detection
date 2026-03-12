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

type Keys struct {
	jwks   *keyfunc.JWKS
	saCert *tls.Certificate
}

const (
	jwksURLFmt = "https://%s/discovery/keys"
	issuerFmt  = "http://%s/services/trust"
)

var (
	logger *zap.Logger

	authConfig struct {
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
	zapCfg := zap.NewProductionConfig()
	if logLevel := os.Getenv("LOG_LEVEL"); logLevel != "" {
		var level zapcore.Level
		if err := level.UnmarshalText([]byte(logLevel)); err == nil {
			zapCfg.Level = zap.NewAtomicLevelAt(level)
		}
	}
	var err error
	logger, err = zapCfg.Build()
	if err != nil {
		panic(err)
	}

	getEnv := func(k string) string {
		v := os.Getenv(k)
		if v == "" {
			logger.Fatal("INIT: Required variable not found in environment",
				zap.String("required", k))
		}
		return v
	}

	idpHost := getEnv("IDP_HOST")
	authorizedHosts := os.Getenv("AUTHORIZED_HOSTS")
	authConfig.audience = getEnv("AUDIENCE")
	authConfig.adminRole = getEnv("ADMIN_ROLE")
	authConfig.publicRole = getEnv("PUBLIC_ROLE")
	authConfig.validateURL = fmt.Sprintf("%s/validate", strings.TrimRight(getEnv("TOKEN_SERVICE_ENDPOINT"), "/"))
	authConfig.secretArn = getEnv("SERVICE_ACCOUNT_SECRET_ARN")
	authConfig.jwksURL = fmt.Sprintf(jwksURLFmt, idpHost)
	authConfig.issuer = fmt.Sprintf(issuerFmt, idpHost)

	if authorizedHosts != "" {
		authConfig.authorizedHosts = strings.Split(authorizedHosts, ",")
		for i := range authConfig.authorizedHosts {
			authConfig.authorizedHosts[i] = strings.TrimSpace(authConfig.authorizedHosts[i])
		}
	}
	logger.Debug("INIT: Authorized read-only hosts", zap.Strings("hosts", authConfig.authorizedHosts))
}

func getTokenType(token string) string {
	if len(token) > 3 && token[:3] == "eyJ" && strings.Count(token, ".") == 2 && token[len(token)-1] != '=' {
		logger.Debug("GetTokenType: Detected JWT")
		return "jwt"
	}
	if strings.ContainsAny(token, "+/=") {
		logger.Debug("GetTokenType: Detected SM")
		return "sm"
	}
	logger.Debug("GetTokenType: No token type detected")
	return ""
}

// Initialize public key cache manager for re-use
func initJWKS() *keyfunc.JWKS {
	start := time.Now()
	jwks, err := keyfunc.Get(authConfig.jwksURL, keyfunc.Options{
		RefreshInterval: time.Hour,
	})
	if err != nil {
		logger.Fatal("Failed to initialize JWKS",
			zap.Error(err),
			zap.String("jwks_url", authConfig.jwksURL),
		)
	}
	logger.Debug("INIT: JWKS  initialized",
		zap.Duration("elapsed", time.Since(start)),
		zap.String("jwks_url", authConfig.jwksURL),
	)
	return jwks
}

// Generate resource policy for a given resource and effect (ie. Allow or Deny)
func generatePolicy(effect, message, userID, role string, event events.APIGatewayCustomAuthorizerRequestTypeRequest) events.APIGatewayCustomAuthorizerResponse {
	sourceIP, _, _ := strings.Cut(event.Headers["CloudFront-Viewer-Address"], ":")
	logger.Info(message,
		zap.String("effect", effect),
		zap.String("user", userID),
		zap.String("role", role),
		zap.String("source_ip", sourceIP),
		zap.String("method", event.HTTPMethod),
		zap.String("path", event.Path),
	)
	return events.APIGatewayCustomAuthorizerResponse{
		PrincipalID: "user",
		PolicyDocument: events.APIGatewayCustomAuthorizerPolicy{
			Version: "2012-10-17",
			Statement: []events.IAMPolicyStatement{{
				Action:   []string{"execute-api:Invoke"},
				Effect:   effect,
				Resource: []string{"*"},
			}},
		},
	}
}

func loadServiceAccountCert() *tls.Certificate {
	logger.Debug("SA_CERT: Loading service account certificate from Secrets Manager",
		zap.String("secret_arn", authConfig.secretArn))

	sess := session.Must(session.NewSession())
	svc := secretsmanager.New(sess)

	result, err := svc.GetSecretValue(&secretsmanager.GetSecretValueInput{
		SecretId: aws.String(authConfig.secretArn),
	})
	if err != nil {
		logger.Fatal("Failed to retrieve service account secret",
			zap.Error(err),
			zap.String("secret_arn", authConfig.secretArn),
		)
	}
	logger.Debug("SA_CERT: Successfully retrieved secret from Secrets Manager",
		zap.Int("secret_length", len(*result.SecretString)))

	var secret struct {
		CertPEM string `json:"cert_pem"`
		KeyPEM  string `json:"key_pem"`
	}
	if err := json.Unmarshal([]byte(*result.SecretString), &secret); err != nil {
		logger.Fatal("Failed to unmarshal service account secret", zap.Error(err))
	}

	cert, err := tls.X509KeyPair([]byte(secret.CertPEM), []byte(secret.KeyPEM))
	if err != nil {
		logger.Fatal("Failed to parse service account X509 key pair", zap.Error(err))
	}

	return &cert
}

func (k *Keys) validateServiceAccountToken(token string) bool {
	logger.Debug("SM_VALIDATE: Starting service account token validation",
		zap.String("token_prefix", token[:min(10, len(token))]),
		zap.Int("token_length", len(token)))
	if k.saCert == nil {
		k.saCert = loadServiceAccountCert()
	}
	logger.Debug("SM_VALIDATE: Certificate loaded successfully, creating HTTP client")

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				Certificates: []tls.Certificate{*k.saCert},
			},
		},
	}

	reqBody, _ := json.Marshal(map[string]string{"token": token})
	logger.Debug("SM_VALIDATE: Sending validation request",
		zap.String("url", authConfig.validateURL),
		zap.Int("request_body_length", len(reqBody)))

	resp, err := client.Post(authConfig.validateURL, "application/json", bytes.NewReader(reqBody))
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
	return false
}

func (k *Keys) parseToken(ctx context.Context, token, tokenType string) (role, userID string) {
	switch tokenType {
	case "jwt":
		if k.jwks == nil {
			k.jwks = initJWKS()
		}
		// Validate token: signature, algorithm, issuer, audience
		parsed, err := jwt.Parse(
			token,
			k.jwks.Keyfunc,
			jwt.WithValidMethods([]string{"RS256"}),
			jwt.WithIssuer(authConfig.issuer),
			jwt.WithAudience(authConfig.audience),
		)
		logger.Debug("Token details", zap.Any("claims", parsed.Claims))
		if err == nil && parsed.Valid {
			claims, _ := parsed.Claims.(jwt.MapClaims)
			role, _ = claims["groups"].(string)
			userID, _ = claims["AgencyUID"].(string)
			return role, userID
		}
		logger.Info("Invalid JWT", zap.Error(err))

	case "sm":
		if k.validateServiceAccountToken(token) {
			return authConfig.publicRole, "Service Account"
		}

	}
	return "", ""
}

func (k *Keys) Handler(ctx context.Context, event events.APIGatewayCustomAuthorizerRequestTypeRequest) (events.APIGatewayCustomAuthorizerResponse, error) {
	logger.Debug("Received event", zap.Any("event", event))
	var role, userID string
	sourceIP, _, _ := strings.Cut(event.Headers["CloudFront-Viewer-Address"], ":")
	if sourceIP == "" {
		sourceIP = event.RequestContext.Identity.SourceIP
		logger.Warn("CloudFront-Viewer-Address header missing, falling back to Identity.SourceIP")
	}

	logger.Info("Recieved request",
		zap.String("source_ip", sourceIP),
		zap.String("method", event.HTTPMethod),
		zap.String("path", event.Path),
	)

	isWhitelisted := slices.Contains(authConfig.authorizedHosts, sourceIP)
	token := strings.TrimPrefix(event.Headers["Authorization"], "Bearer ")

	// Validate for admin if jwt or public if sm and not whitelisted
	if tt := getTokenType(token); tt == "jwt" || (tt == "sm" && !isWhitelisted) {
		role, userID = k.parseToken(ctx, token, tt)
	}
	if role == "" && isWhitelisted {
		role = authConfig.publicRole
		userID = sourceIP
	}

	// Allow all admin requests
	if role == authConfig.adminRole {
		return generatePolicy("Allow", "Authorizing admin request", userID, role, event), nil
	}

	// Allow public requests for GET
	if event.HTTPMethod == "GET" && role == authConfig.publicRole {
		return generatePolicy("Allow", "Authorizing public request", userID, role, event), nil
	}

	// Default deny catch-all
	return generatePolicy("Deny", "Unauthorized request", userID, role, event), nil
}

func main() {
	lambda.Start((&Keys{}).Handler)
}
