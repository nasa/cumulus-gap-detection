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
)

const (
	authorizationClaim = "roles"
	adminValue         = "admin"
	publicValue        = "public"
)

var (
	jwks     *keyfunc.JWKS
	jwksOnce sync.Once
	jwksErr  error
)

func initJWKS() error {
	jwksOnce.Do(func() {
		jwks, jwksErr = keyfunc.Get(os.Getenv("JWKS_URL"), keyfunc.Options{
			RefreshInterval: time.Hour,
		})
	})
	return jwksErr
}

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
		return generatePolicy("Deny", event.MethodArn), err
	}

	parsed, err := jwt.Parse(
		strings.TrimPrefix(event.Headers["Authorization"], "Bearer "),
		jwks.Keyfunc,
		jwt.WithValidMethods([]string{"RS256"}),
		jwt.WithIssuer(os.Getenv("ISSUER")),
	)

	if err != nil || !parsed.Valid {
		return generatePolicy("Deny", event.MethodArn), nil
	}

	claims, _ := parsed.Claims.(jwt.MapClaims)
	authValue, _ := claims[authorizationClaim].(string)

	if event.HTTPMethod == "POST" && authValue != adminValue {
		return generatePolicy("Deny", event.MethodArn), nil
	}

	if authValue != adminValue && authValue != publicValue {
		return generatePolicy("Deny", event.MethodArn), nil
	}

	return generatePolicy("Allow", event.MethodArn), nil
}

func main() {
	lambda.Start(Handler)
}
