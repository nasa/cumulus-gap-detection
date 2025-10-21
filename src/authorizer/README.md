# Testing
Verbose: go test -v -cover -coverprofile=coverage.out
Minimal: go test -coverprofile=coverage.out
## Coverage Report
terminal: go tool cover -func=coverage.out
html: go tool cover -html=coverage.out
