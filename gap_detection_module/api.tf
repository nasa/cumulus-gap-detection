resource "aws_api_gateway_rest_api" "api" {
  name = "${var.DEPLOY_NAME}-TemporalGapAPI"
  body = templatefile("${path.module}/openapi.yaml", local.openapi_template_vars)
  #endpoint_configuration {
  #  types           = ["PRIVATE"]
  #  ip_address_type = "dualstack" # Required for Private API
  #}
}

# This is a placeholder policy attached to the API to bypass a "Private REST API doesn't have a resource policy attached to it" error. 
# NGAP takes care of automatically adding a resource policy to the API in their cron job lambda
resource "aws_api_gateway_rest_api_policy" "initial_api_gateway_policy" {
  rest_api_id = aws_api_gateway_rest_api.api.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "NoEffectPlaceholder"
        Effect    = "Allow"
        Action    = []
        Resource  = "*"
        Principal = "*"
      }
    ]
  })
}
resource "aws_api_gateway_deployment" "deployment" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  triggers = {
    redeployment = sha1(templatefile("${path.module}/openapi.yaml", local.openapi_template_vars))
  }
  lifecycle {
    create_before_destroy = true # This will allow the following sequence: new deployment->point stage to new deployment -> deleted the old deployment
  }
  depends_on = [
    aws_api_gateway_rest_api_policy.initial_api_gateway_policy
  ]


}

resource "aws_api_gateway_stage" "stage" {
  deployment_id = aws_api_gateway_deployment.deployment.id
  rest_api_id   = aws_api_gateway_rest_api.api.id
  stage_name    = "${var.DEPLOY_NAME}-TemporalGapAPI"
}

#resource "aws_api_gateway_authorizer" "authorizer" {
#  name                   = "${var.DEPLOY_NAME}-Authorizer"
#  rest_api_id            = aws_api_gateway_rest_api.api.id
#  authorizer_uri         = aws_lambda_function.authorizer.invoke_arn
#  # authorizer_credentials = var.api_gateway_authorizer_role_arn 
#  type                   = "REQUEST"
#  identity_source        = "method.request.header.Authorization"
#}
resource "aws_lambda_permission" "authorizer_permission" {
  statement_id  = "AllowAPIGatewayInvokeAuthorizer"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.authorizer.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.api.execution_arn}/authorizers/*"
}
#
#resource "aws_lambda_permission" "authorizer_permission" {
#  statement_id  = "AllowAPIGatewayInvokeAuthorizer"
#  action        = "lambda:InvokeFunction"
#  function_name = aws_lambda_function.authorizer.function_name
#  principal     = "apigateway.amazonaws.com"
#  source_arn    = "${aws_api_gateway_rest_api.api.execution_arn}/authorizers/${aws_api_gateway_authorizer.authorizer.id}"
#}
