package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"reflect"
	"testing"
	"time"

	"go.uber.org/zap"
	"github.com/MicahParks/keyfunc/v2"
	"github.com/aws/aws-lambda-go/events"
	"github.com/golang-jwt/jwt/v5"
)

const (
	testMethodArn      = "arn:aws:execute-api:us-west-2:123456789012:abcdef/test/GET/foo"
	authorizationClaim = "groups"
	testAudience       = "example-aud-1234"
)

var (
	testPrivateKey *rsa.PrivateKey
	testPublicKey  *rsa.PublicKey
	testJWKS       *keyfunc.JWKS
	adminRole      string
	publicRole     string
	testIssuer     string

	_ = func() bool {
		os.Setenv("IDP_HOST", "example.idp.org")
		os.Setenv("AUDIENCE", testAudience)
		os.Setenv("ADMIN_ROLE", "admin")
		os.Setenv("PUBLIC_ROLE", "public")
		os.Setenv("AUTHORIZED_HOSTS", "999.999.999.999")
		os.Setenv("TOKEN_SERVICE_ENDPOINT", "https://token-service.example.internal")
		os.Setenv("SERVICE_ACCOUNT_SECRET_ARN", "arn:aws:secretsmanager:us-west-2:123456789012:secret:test-sa-cert")
		return true
	}()
)

func TestMain(m *testing.M) {
	var err error
	testPrivateKey, err = rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	testPublicKey = &testPrivateKey.PublicKey

	testHost := "example.idp.org"
	testJWKS, err = keyfunc.NewJSON(json.RawMessage(fmt.Sprintf(`{
		"keys": [{
			"kty": "RSA",
			"alg": "RS256",
			"kid": "test-key",
			"use": "sig",
			"n": "%s",
			"e": "%s"
		}]
	}`,
		base64.RawURLEncoding.EncodeToString(testPublicKey.N.Bytes()),
		base64.RawURLEncoding.EncodeToString(big.NewInt(int64(testPublicKey.E)).Bytes()),
	)))
	if err != nil {
		panic(err)
	}

	adminRole = os.Getenv("ADMIN_ROLE")
	publicRole = os.Getenv("PUBLIC_ROLE")

	authConfig.adminRole = "admin"
	authConfig.publicRole = "public"
	authConfig.audience = testAudience
	testIssuer = fmt.Sprintf(issuerFmt, testHost)
	authConfig.issuer = testIssuer
	authConfig.authorizedHosts = []string{"999.999.999.999"}
	authConfig.validateURL = "https://token-service.example.internal/validate"
	authConfig.secretArn = "arn:aws:secretsmanager:us-west-2:123456789012:secret:test-sa-cert"
	logger = zap.NewNop()

	os.Exit(m.Run())
}

func createToken(claims jwt.MapClaims) string {
	if claims["iss"] == nil {
		claims["iss"] = testIssuer
	}
	if claims["aud"] == nil {
		claims["aud"] = testAudience
	}
	if claims["exp"] == nil {
		claims["exp"] = time.Now().Add(time.Hour).Unix()
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = "test-key"

	signed, err := token.SignedString(testPrivateKey)
	if err != nil {
		panic(err)
	}
	return signed
}

func newTestKeys() *Keys {
	return &Keys{jwks: testJWKS}
}

func TestGeneratePolicy(t *testing.T) {
	tests := []struct {
		name    string
		effect  string
		message string
		userID  string
		role    string
		event   events.APIGatewayCustomAuthorizerRequestTypeRequest
		want    events.APIGatewayCustomAuthorizerResponse
	}{
		{
			name:    "allow",
			effect:  "Allow",
			message: "test allow",
			userID:  "testuser",
			role:    "admin",
			event: events.APIGatewayCustomAuthorizerRequestTypeRequest{
				MethodArn:  testMethodArn,
				HTTPMethod: "GET",
				Path:       "/test",
				RequestContext: events.APIGatewayCustomAuthorizerRequestTypeRequestContext{
					Identity: events.APIGatewayCustomAuthorizerRequestTypeRequestIdentity{
						SourceIP: "0.0.0.0",
					},
				},
			},
			want: events.APIGatewayCustomAuthorizerResponse{
				PrincipalID: "user",
				PolicyDocument: events.APIGatewayCustomAuthorizerPolicy{
					Version: "2012-10-17",
					Statement: []events.IAMPolicyStatement{{
						Action:   []string{"execute-api:Invoke"},
						Effect:   "Allow",
						Resource: []string{testMethodArn},
					}},
				},
			},
		},
		{
			name:    "deny",
			effect:  "Deny",
			message: "test deny",
			userID:  "testuser",
			role:    "public",
			event: events.APIGatewayCustomAuthorizerRequestTypeRequest{
				MethodArn:  testMethodArn,
				HTTPMethod: "POST",
				Path:       "/test",
				RequestContext: events.APIGatewayCustomAuthorizerRequestTypeRequestContext{
					Identity: events.APIGatewayCustomAuthorizerRequestTypeRequestIdentity{
						SourceIP: "0.0.0.0",
					},
				},
			},
			want: events.APIGatewayCustomAuthorizerResponse{
				PrincipalID: "user",
				PolicyDocument: events.APIGatewayCustomAuthorizerPolicy{
					Version: "2012-10-17",
					Statement: []events.IAMPolicyStatement{{
						Action:   []string{"execute-api:Invoke"},
						Effect:   "Deny",
						Resource: []string{"*"},
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := generatePolicy(tt.effect, tt.message, tt.userID, tt.role, tt.event)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mismatch:\ngot:  %+v\nwant: %+v", got, tt.want)
			}
		})
	}
}

func TestHandler(t *testing.T) {
	tests := []struct {
		name       string
		headers    map[string]string
		httpMethod string
		path       string
		sourceIP   string
		wantEffect string
	}{
		{
			name:       "missing token",
			headers:    map[string]string{},
			httpMethod: "GET",
			path:       "/getTimeGaps",
			wantEffect: "Deny",
		},
		{
			name:       "malformed token",
			headers:    map[string]string{"Authorization": "NotBearer xyz"},
			httpMethod: "GET",
			path:       "/getTimeGaps",
			wantEffect: "Deny",
		},
		{
			name:       "expired token",
			headers:    map[string]string{"Authorization": "Bearer " + createToken(jwt.MapClaims{authorizationClaim: adminRole, "exp": time.Now().Add(-time.Hour).Unix()})},
			httpMethod: "GET",
			path:       "/getTimeGaps",
			wantEffect: "Deny",
		},
		{
			name:       "wrong issuer",
			headers:    map[string]string{"Authorization": "Bearer " + createToken(jwt.MapClaims{authorizationClaim: adminRole, "iss": "https://wrong.issuer"})},
			httpMethod: "GET",
			path:       "/getTimeGaps",
			wantEffect: "Deny",
		},
		{
			name:       "admin on POST",
			headers:    map[string]string{"Authorization": "Bearer " + createToken(jwt.MapClaims{authorizationClaim: adminRole})},
			httpMethod: "POST",
			path:       "/gapConfig",
			wantEffect: "Allow",
		},
		{
			name:       "public on POST",
			headers:    map[string]string{"Authorization": "Bearer " + createToken(jwt.MapClaims{authorizationClaim: publicRole})},
			httpMethod: "POST",
			path:       "/gapConfig",
			wantEffect: "Deny",
		},
		{
			name:       "admin on GET",
			headers:    map[string]string{"Authorization": "Bearer " + createToken(jwt.MapClaims{authorizationClaim: adminRole})},
			httpMethod: "GET",
			path:       "/getTimeGaps",
			wantEffect: "Allow",
		},
		{
			name:       "public on GET",
			headers:    map[string]string{"Authorization": "Bearer " + createToken(jwt.MapClaims{authorizationClaim: publicRole})},
			httpMethod: "GET",
			path:       "/getTimeGaps",
			wantEffect: "Allow",
		},
		{
			name:       "invalid role",
			headers:    map[string]string{"Authorization": "Bearer " + createToken(jwt.MapClaims{authorizationClaim: "invalid"})},
			httpMethod: "GET",
			path:       "/getTimeGaps",
			wantEffect: "Deny",
		},
		{
			name:       "missing role claim",
			headers:    map[string]string{"Authorization": "Bearer " + createToken(jwt.MapClaims{})},
			httpMethod: "GET",
			path:       "/getTimeGaps",
			wantEffect: "Deny",
		},
		{
			name:       "whitelisted IP no token GET",
			headers:    map[string]string{"CloudFront-Viewer-Address": "999.999.999.999:12345"},
			httpMethod: "GET",
			path:       "/getTimeGaps",
			wantEffect: "Allow",
		},
		{
			name:       "whitelisted IP no token POST",
			headers:    map[string]string{"CloudFront-Viewer-Address": "999.999.999.999:12345"},
			httpMethod: "POST",
			path:       "/gapConfig",
			wantEffect: "Deny",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := newTestKeys().Handler(context.Background(), events.APIGatewayCustomAuthorizerRequestTypeRequest{
				MethodArn:  testMethodArn,
				HTTPMethod: tt.httpMethod,
				Path:       tt.path,
				Headers:    tt.headers,
			})

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if resp.PolicyDocument.Statement[0].Effect != tt.wantEffect {
				t.Errorf("expected %s, got %s", tt.wantEffect, resp.PolicyDocument.Statement[0].Effect)
			}
		})
	}
}
