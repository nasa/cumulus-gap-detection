# Authorizer

This is a Go module implementing a Lambda function for AWS API Gateway's Lambda Authorizer (https://docs.aws.amazon.com/apigateway/latest/developerguide/apigateway-use-lambda-authorizer.html), a stateless Policy Desicion Point (PDP) that provides authorization verdicts to API gateway.

## Structure
authroizer.go: The source code of the Lambda Authorizer, including the handler and authorization logic.
go.mod: The dependencies required for the authorizer.
go.sum: The hashes of each dependency that are verfied at build time before the binary is built
authroizer_test.go: Unit tests for the authorizer. Tests can be run with $ go test from the module root.

## Authorization Logic
Authorization depends on the identity source of the request.Three distinct sources are supported: Launchpad JWT, Launchpad (SiteMinder) Service Account token, and IP address. Because API Gateway denys all requests lacking the configured identity source without consulting the Lambda Authorizer, the Authorization header MUST be set for any authorizatio, even when the source IP is whitelisted. The value of the token does not matter so long as it is not empty. In order to determine which source to use, a series of checks on the Authorization header are performed:
1. If there is no Authentication header or the Bearer field is empty, a Deny verdict is returned.
2. The token string is evaluated against constant properties of the JWT spec.
3. If the token is not a JWT as determined in 2, then any standard Base64 (as opposed to Base64URL) string is classified as a SiteMinder token (aka Service Account)
4. All other strings are not classified as access tokens to avoid time-consuming validation of arbitrary crednetials. 
