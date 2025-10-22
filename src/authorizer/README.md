# Testing
## Minimal
go test -coverprofile=coverage.out && go tool cover -func=coverage.out
## Detailed
go test -v -cover -coverprofile=coverage.out && go tool cover -html=coverage.out

# TODO
- Deployment
    - Add authorizer to deployment template
    - Add build logic to packaging script
    - Update API spec to designate routes as requiring auth
    - Import API spec to APIGW resource
    - Optimize build/deployment artifact size
    - architecture amd vs arm

# Build script
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build \
  -ldflags="-s -w" \
  -o bootstrap \
  authorizer.go
zip authorizer.zip bootstrap
