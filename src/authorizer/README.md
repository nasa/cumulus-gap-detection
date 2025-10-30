# Testing
## Minimal
go test -coverprofile=coverage.out && go tool cover -func=coverage.out
## Detailed
go test -v -cover -coverprofile=coverage.out && go tool cover -html=coverage.out

# TODO
- Use secrets manager for sensitive configs
- Update deployment repo module with new vars
- Add unit tests in CI
