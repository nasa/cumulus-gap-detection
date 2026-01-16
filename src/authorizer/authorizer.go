package main

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/MicahParks/keyfunc/v2"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/golang-jwt/jwt/v5"
	"go.uber.org/zap"
)

const (
	jwksURLFmt = "https://%s/discovery/keys"
	issuerFmt  = "http://%s/services/trust"
)

var (
	jwks     *keyfunc.JWKS
	jwksOnce sync.Once
	jwksErr  error
	logger   *zap.Logger

	config struct {
		jwksURL         string
		issuer          string
		audience        string
		adminRole       string
		publicRole      string
		authorizedHosts []string
	}
)

func init() {
	var err error
	logger, err = zap.NewDevelopment()
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
				logger.Fatal("Required variable not found in environment",
					zap.String("required", k))
			}
			return v
		}
		// Set backend config from environment variables
		idpHost := getEnv("IDP_HOST")
		authorizedHosts := getEnv("AUTHORIZED_HOSTS")
		config.audience = getEnv("AUDIENCE")
		config.adminRole = getEnv("ADMIN_ROLE")
		config.publicRole = getEnv("PUBLIC_ROLE")
		config.jwksURL = fmt.Sprintf(jwksURLFmt, idpHost)
		config.issuer = fmt.Sprintf(issuerFmt, idpHost)

		// Parse IP addresses from whitelist env var
		if authorizedHosts != "" {
			config.authorizedHosts = strings.Split(authorizedHosts, ",")
			for i := range config.authorizedHosts {
				config.authorizedHosts[i] = strings.TrimSpace(config.authorizedHosts[i])
			}
		}
		logger.Debug("Authorized read-only hosts", zap.Strings("ips", config.authorizedHosts))
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
		logger.Debug("JWKS fetch completed",
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

func parseToken(authHeader string) (role, userID string) {
	// Extract bearer token
	token := strings.TrimPrefix(authHeader, "Bearer ")
	if token == "" || token == authHeader {
		logger.Info("Malformed Authorization header")
		return "", ""
	}

	// Initialize JWKS
	if err := initJWKS(); err != nil {
		logger.Error("JWKS initialization failed",
			zap.Error(err),
			zap.String("jwks_url", config.jwksURL),
		)
		return "", ""
	}

	// Validate token: signature, algorithm, issuer, audience
	parsed, err := jwt.Parse(
		token,
		jwks.Keyfunc,
		jwt.WithValidMethods([]string{"RS256"}),
		jwt.WithIssuer(config.issuer),
		jwt.WithAudience(config.audience),
	)

	if err != nil || !parsed.Valid {
		logger.Info("Invalid token", zap.Error(err))
		return "", ""
	}

	claims, _ := parsed.Claims.(jwt.MapClaims)
	role, _ = claims["groups"].(string)
	userID, _ = claims["AgencyUID"].(string)
	return role, userID
}

func Handler(ctx context.Context, event events.APIGatewayCustomAuthorizerRequestTypeRequest) (events.APIGatewayCustomAuthorizerResponse, error) {
	initConfig()
	var role, userID string
	sourceIP := event.RequestContext.Identity.SourceIP
	authHeader := event.Headers["Authorization"]

	logger.Debug("Got request: ",
		zap.String("source_ip", sourceIP),
		zap.String("method", event.HTTPMethod),
		zap.String("path", event.Path),
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
