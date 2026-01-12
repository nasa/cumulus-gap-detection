package main

import (
	"context"
	"fmt"
	"os"
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
		jwksURL    string
		issuer     string
		audience   string
		adminRole  string
		publicRole string
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
		idpHost := getEnv("IDP_HOST")
		config.jwksURL = fmt.Sprintf(jwksURLFmt, idpHost)
		config.issuer = fmt.Sprintf(issuerFmt, idpHost)
		config.audience = getEnv("AUDIENCE")
		config.adminRole = getEnv("ADMIN_ROLE")
		config.publicRole = getEnv("PUBLIC_ROLE")
	})
}

// Initialize public key cache manager for re-use
func initJWKS() error {
	jwksOnce.Do(func() {
		jwks, jwksErr = keyfunc.Get(os.Getenv("JWKS_URL"), keyfunc.Options{
			RefreshInterval: time.Hour,
		})
	})
	return jwksErr
}

// Generate resource policy for a given resource and effect (ie. Allow or Deny)
func generatePolicy(effect, resource string) events.APIGatewayCustomAuthorizerResponse {
	if effect == "Deny" {
		resource = "*"
	}
	return events.APIGatewayCustomAuthorizerResponse{
		PrincipalID: "user",
		PolicyDocument: events.APIGatewayCustomAuthorizerPolicy{
			Version: "2012-10-17",
			Statement: []events.IAMPolicyStatement{{
				Action:   []string{"execute-api:Invoke"},
				Effect:   effect,
				Resource: []string{resource},
			}},
		},
	}
}

func Handler(ctx context.Context, event events.APIGatewayCustomAuthorizerRequestTypeRequest) (events.APIGatewayCustomAuthorizerResponse, error) {
	initConfig()
	// Initialize JWKS Client once, caches public key in lambda runtime
	if err := initJWKS(); err != nil {
		logger.Error("JWKS initialization failed",
			zap.Error(err),
			zap.String("jwks_url", os.Getenv("JWKS_URL")),
		)
		return generatePolicy("Deny", event.MethodArn), err
	}

	// Parse and validate using IdP public key
	parsed, err := jwt.Parse(
		strings.TrimPrefix(event.Headers["Authorization"], "Bearer "),
		jwks.Keyfunc,
		jwt.WithValidMethods([]string{"RS256"}),
		jwt.WithIssuer(os.Getenv("ISSUER")),
	)
	logger.Debug("Token parsed",
		zap.Any("token", parsed),
		zap.Error(err),
	)

	// Deny if parsing failure or invalid attributes
	if err != nil || !parsed.Valid {
		logger.Info("Token validation failed",
			zap.String("method", event.HTTPMethod),
			zap.String("path", event.Path),
			zap.Error(err),
		)
		return generatePolicy("Deny", event.MethodArn), nil
	}

	// Extract role from valid token
	claims, _ := parsed.Claims.(jwt.MapClaims)
	role, _ := claims["groups"].(string)

	// Deny if role claim is missing or empty
	if role == "" {
		logger.Info("Missing or empty role claim",
			zap.String("method", event.HTTPMethod),
			zap.String("path", event.Path),
		)
		return generatePolicy("Deny", event.MethodArn), nil
	}

	// Deny if lacking permission for route
	if event.HTTPMethod == "POST" && role != config.adminRole {
		logger.Info("Unauthorized request for admin",
			zap.String("role", role),
			zap.String("method", event.HTTPMethod),
			zap.String("path", event.Path),
		)
		return generatePolicy("Deny", event.MethodArn), nil
	}

	if role != config.adminRole && role != config.publicRole {
		logger.Info("Unauthorized request",
			zap.String("role", role),
			zap.String("method", event.HTTPMethod),
			zap.String("path", event.Path),
		)
		return generatePolicy("Deny", event.MethodArn), nil
	}

	return generatePolicy("Allow", event.MethodArn), nil
}

func main() {
	defer logger.Sync()
	lambda.Start(Handler)
}
