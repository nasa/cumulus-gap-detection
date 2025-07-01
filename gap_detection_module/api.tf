resource "aws_api_gateway_rest_api" "api" {
  name = "${var.DEPLOY_NAME}-TemporalGapAPI"
  body = templatefile("${path.module}/openapi.yaml", local.openapi_template_vars)
  endpoint_configuration {
    types           = ["PRIVATE"]
    ip_address_type = "dualstack" # Required for Private API
  }
}

resource "aws_api_gateway_deployment" "deployment" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  triggers = {
    redeployment = sha1(templatefile("${path.module}/openapi.yaml", local.openapi_template_vars))
  }
  lifecycle {
    create_before_destroy = true # This will allow the following sequence: new deployment->point stage to new deployment -> deleted the old deployment
  }
}

resource "aws_api_gateway_stage" "stage" {
  deployment_id = aws_api_gateway_deployment.deployment.id
  rest_api_id   = aws_api_gateway_rest_api.api.id
  stage_name    = "${var.DEPLOY_NAME}-TemporalGapAPI"
}
