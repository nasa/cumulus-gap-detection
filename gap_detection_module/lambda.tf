resource "aws_lambda_function" "gap_functions" {
  for_each         = local.gap_functions
  depends_on       = [aws_rds_cluster_instance.cluster_instances]
  function_name    = "${var.DEPLOY_NAME}-${each.key}"
  filename         = "${path.module}/artifacts/functions/${each.key}.zip"
  source_code_hash = filebase64sha256("${path.module}/artifacts/functions/${each.key}.zip")
  role             = var.lambda_processing_role_arn
  runtime          = "python3.13"
  handler          = "${each.key}.lambda_handler"
  timeout          = lookup(each.value, "timeout", 10)
  memory_size      = lookup(each.value, "memory_size", 128)
  # Default no reserved concurrency
  reserved_concurrent_executions = lookup(each.value, "reserved_concurrent_executions ", -1)
  vpc_config {
    subnet_ids         = var.subnet_ids
    security_group_ids = var.security_group_ids
  }
  # Include function-specific layer only if the deps file exists
  layers = contains(keys(local.functions_with_deps), each.key) ? [
    aws_lambda_layer_version.function_layers[each.key].arn,
    aws_lambda_layer_version.utils_layer.arn
    ] : [
    aws_lambda_layer_version.utils_layer.arn
  ]
  # Populate env varirables if present
  dynamic "environment" {
    for_each = contains(keys(each.value), "variables") ? [1] : []
    content {
      variables = each.value.variables
    }
  }
}

resource "aws_s3_object" "function_deps" {
  for_each    = local.functions_with_deps
  bucket      = aws_s3_bucket.artifacts_bucket
  key         = "${each.key}-deps.zip"
  source      = "${path.module}/artifacts/layers/${each.key}-deps.zip"
  source_hash = filemd5("${path.module}/artifacts/layers/${each.key}-deps.zip")
}

resource "aws_lambda_layer_version" "function_layers" {
  for_each            = local.functions_with_deps
  layer_name          = "${var.DEPLOY_NAME}-${each.key}_layer"
  s3_bucket           = aws_s3_bucket.artifacts_bucket
  s3_key              = aws_s3_object.function_deps[each.key].key
  source_code_hash    = filebase64sha256("${path.module}/artifacts/layers/${each.key}-deps.zip")
  compatible_runtimes = ["python3.13"]
}

resource "aws_lambda_layer_version" "utils_layer" {
  layer_name          = "${var.DEPLOY_NAME}-utils_layer"
  filename            = "${path.module}/artifacts/layers/utils-deps.zip"
  source_code_hash    = filebase64sha256("${path.module}/artifacts/layers/utils-deps.zip")
  compatible_runtimes = ["python3.13"]
}

# gapDetectionIngestQueue trigger for gapUpdate lambda function
resource "aws_lambda_event_source_mapping" "ingest_sqs_trigger" {
  event_source_arn                   = aws_sqs_queue.gap_detection_ingest_queue.arn
  function_name                      = aws_lambda_function.gap_functions["gapUpdate"].arn
  batch_size                         = var.sqs_trigger_process_gaps_batch_size
  maximum_batching_window_in_seconds = var.sqs_trigger_max_batch_window
  # Allows valid collections to be processed from batch containing invalid collections
  function_response_types = ["ReportBatchItemFailures"]
  # Prevent over-invocation to avoid contention
  scaling_config {
    maximum_concurrency = 16
  }
}

# gapDetectionDeletionQueue trigger for gapUpdate lambda function
resource "aws_lambda_event_source_mapping" "deletion_sqs_trigger" {

  event_source_arn = aws_sqs_queue.gap_detection_deletion_queue.arn

  function_name = aws_lambda_function.gap_functions["gapUpdate"].arn

  function_response_types = ["ReportBatchItemFailures"]

  # TODO Refine params

  batch_size = var.sqs_trigger_process_gaps_batch_size

  maximum_batching_window_in_seconds = var.sqs_trigger_max_batch_window

  # Allows valid collections to be processed from batch containing invalid collections

  # Prevent over-invocation to avoid contention

  scaling_config {

    maximum_concurrency = 16

  }

}

# Allow lambda role to connect to RDS proxy
resource "aws_iam_role_policy" "lambda_rds_proxy_role_policy" {
  name = "${var.DEPLOY_NAME}-gap-detection-rds-proxy-policy"
  role = var.lambda_processing_role_name
  policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        Sid    = "AllowRDSProxyConnection"
        Effect = "Allow"
        Action = [
          "rds-db:connect"
        ]
        Resource = aws_db_proxy.rds_proxy.arn
      }
    ]
  })
}

# Allow lambda role to read RDS secrets from secrets manager
resource "aws_iam_role_policy" "lambda_rds_secret_policy" {
  name = "${var.DEPLOY_NAME}-gap-detection-rds-secret-policy"
  role = var.lambda_processing_role_name
  policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        Sid    = "AllowReadRDSSecret"
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = aws_secretsmanager_secret.rds_admin_login.arn
      }
    ]
  })
}

# Allow lambda security group to connect to RDS proxy
resource "aws_security_group_rule" "lambda_to_proxy" {
  type                     = "egress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.rds_proxy_sg.id
  security_group_id        = var.security_group_id
}

# Lambda permissions for API gateway 
resource "aws_lambda_permission" "api_gateway_permissions" {
  for_each = toset(local.api_functions)

  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.gap_functions[each.key].function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.api.execution_arn}/*/*/${each.key}"
}
