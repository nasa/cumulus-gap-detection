// main_test.go
package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/golang-jwt/jwt/v5"
)

const (
	testMethodArn = "arn:aws:execute-api:us-east-1:123456789012:abcdef/prod/GET/getTimeGaps"
	testIssuer    = "https://test.issuer.example"
)

var (
	testPrivateKey *rsa.PrivateKey
	testPublicKey  *rsa.PublicKey
	jwksServer     *httptest.Server
)

func TestMain(m *testing.M) {
	var err error
	testPrivateKey, err = rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	testPublicKey = &testPrivateKey.PublicKey

	jwksServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nBytes := testPublicKey.N.Bytes()
		eBytes := big.NewInt(int64(testPublicKey.E)).Bytes()

		jwks := map[string]interface{}{
			"keys": []map[string]string{
				{
					"kty": "RSA",
					"alg": "RS256",
					"kid": "test-key",
					"use": "sig",
					"n":   base64.RawURLEncoding.EncodeToString(nBytes),
					"e":   base64.RawURLEncoding.EncodeToString(eBytes),
				},
			},
		}
		json.NewEncoder(w).Encode(jwks)
	}))

	os.Setenv("JWKS_URL", jwksServer.URL)
	os.Setenv("ISSUER", testIssuer)

	code := m.Run()

	jwksServer.Close()
	os.Exit(code)
}

func createToken(role string) string {
	claims := jwt.MapClaims{
		authorizationClaim: role,
		"iss":              testIssuer,
		"exp":              time.Now().Add(time.Hour).Unix(),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = "test-key"

	signed, err := token.SignedString(testPrivateKey)
	if err != nil {
		panic(err)
	}
	return signed
}

func createTokenWithClaims(claims jwt.MapClaims) string {
	if claims["iss"] == nil {
		claims["iss"] = testIssuer
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

func TestGeneratePolicy(t *testing.T) {
	tests := []struct {
		name     string
		effect   string
		resource string
		want     events.APIGatewayCustomAuthorizerResponse
	}{
		{
			name:     "allow",
			effect:   "Allow",
			resource: testMethodArn,
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
			name:     "deny",
			effect:   "Deny",
			resource: testMethodArn,
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
			got := generatePolicy(tt.effect, tt.resource)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mismatch:\ngot:  %+v\nwant: %+v", got, tt.want)
			}
		})
	}
}

func TestHandler_InitJWKSFailure(t *testing.T) {
	originalURL := os.Getenv("JWKS_URL")
	os.Setenv("JWKS_URL", "bad url")

	jwks = nil
	jwksOnce = sync.Once{}

	defer func() {
		os.Setenv("JWKS_URL", originalURL)
		jwks = nil
		jwksOnce = sync.Once{}
	}()

	resp, err := Handler(context.Background(), events.APIGatewayCustomAuthorizerRequestTypeRequest{
		MethodArn:  testMethodArn,
		HTTPMethod: "GET",
		Path:       "/getTimeGaps",
		Headers:    map[string]string{"Authorization": "Bearer " + createToken(publicValue)},
	})

	if err == nil {
		t.Fatal("expected error from initJWKS failure")
	}

	if resp.PolicyDocument.Statement[0].Effect != "Deny" {
		t.Errorf("expected Deny on JWKS failure, got %s", resp.PolicyDocument.Statement[0].Effect)
	}
}

func TestHandler(t *testing.T) {
	tests := []struct {
		name       string
		headers    map[string]string
		httpMethod string
		path       string
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
			headers:    map[string]string{"Authorization": "Bearer " + createTokenWithClaims(jwt.MapClaims{authorizationClaim: adminValue, "exp": time.Now().Add(-time.Hour).Unix()})},
			httpMethod: "GET",
			path:       "/getTimeGaps",
			wantEffect: "Deny",
		},
		{
			name:       "wrong issuer",
			headers:    map[string]string{"Authorization": "Bearer " + createTokenWithClaims(jwt.MapClaims{authorizationClaim: adminValue, "iss": "https://wrong.issuer"})},
			httpMethod: "GET",
			path:       "/getTimeGaps",
			wantEffect: "Deny",
		},
		{
			name:       "admin on POST",
			headers:    map[string]string{"Authorization": "Bearer " + createToken(adminValue)},
			httpMethod: "POST",
			path:       "/gapConfig",
			wantEffect: "Allow",
		},
		{
			name:       "public on POST",
			headers:    map[string]string{"Authorization": "Bearer " + createToken(publicValue)},
			httpMethod: "POST",
			path:       "/gapConfig",
			wantEffect: "Deny",
		},
		{
			name:       "admin on GET",
			headers:    map[string]string{"Authorization": "Bearer " + createToken(adminValue)},
			httpMethod: "GET",
			path:       "/getTimeGaps",
			wantEffect: "Allow",
		},
		{
			name:       "public on GET",
			headers:    map[string]string{"Authorization": "Bearer " + createToken(publicValue)},
			httpMethod: "GET",
			path:       "/getTimeGaps",
			wantEffect: "Allow",
		},
		{
			name:       "invalid role",
			headers:    map[string]string{"Authorization": "Bearer " + createToken("invalid")},
			httpMethod: "GET",
			path:       "/getTimeGaps",
			wantEffect: "Deny",
		},
		{
			name:       "missing role claim",
			headers:    map[string]string{"Authorization": "Bearer " + createTokenWithClaims(jwt.MapClaims{})},
			httpMethod: "GET",
			path:       "/getTimeGaps",
			wantEffect: "Deny",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := Handler(context.Background(), events.APIGatewayCustomAuthorizerRequestTypeRequest{
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
