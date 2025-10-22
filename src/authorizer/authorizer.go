package main

import (
	"context"
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

// Generic placeholders until IdP is set up
const (
	authorizationClaim = "roles"
	adminValue         = "admin"
	publicValue        = "public"
)

var (
	jwks     *keyfunc.JWKS
	jwksOnce sync.Once
	jwksErr  error
	logger   *zap.Logger
)


func init() {
	var err error
	logger, err = zap.NewProduction()
	if err != nil {
		panic(err)
	}
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
	if err := initJWKS(); err != nil {
		logger.Error("JWKS initialization failed",
			zap.Error(err),
			zap.String("jwks_url", os.Getenv("JWKS_URL")),
		)
		return generatePolicy("Deny", event.MethodArn), err
	}

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

	if err != nil || !parsed.Valid {
		logger.Info("Token validation failed",
			zap.String("method", event.HTTPMethod),
			zap.String("path", event.Path),
			zap.Error(err),
		)
		return generatePolicy("Deny", event.MethodArn), nil
	}

	claims, _ := parsed.Claims.(jwt.MapClaims)
	authValue, _ := claims[authorizationClaim].(string)

	if event.HTTPMethod == "POST" && authValue != adminValue {
		// log info
		logger.Info("Unauthorized request for admin",
			zap.String("role", authValue),
			zap.String("method", event.HTTPMethod),
			zap.String("path", event.Path),
		)
		return generatePolicy("Deny", event.MethodArn), nil
	}

	if authValue != adminValue && authValue != publicValue {
		logger.Info("Unauthorized request",
			zap.String("role", authValue),
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
