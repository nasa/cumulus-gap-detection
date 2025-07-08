
output "DEPLOY_NAME" {
  description = "Deployment name used as a prefix for gap detection resources"
  value       = var.DEPLOY_NAME
}
output "api_gateway_id" {
  description = "Temporal Gap APIGATEWAY ID"
  value       = aws_api_gateway_deployment.deployment.rest_api_id
}
output "report_granules_deletion_subscription_arn" {
  description = "Report granules deletion subscription ARN"
  value       = aws_sns_topic_subscription.report_granules_deletion_subscription.arn
}
output "report_granules_ingest_subscription_arn" {
  description = "Report granules ingest subscription ARN"
  value       = aws_sns_topic_subscription.report_granules_ingest_subscription.arn
}
output "rds_cluster_arn" {
  value       = aws_rds_cluster.rds_cluster.arn
  description = "ARN of the RDS Cluster"
}
output "rds_endpoint" {
  value       = aws_rds_cluster.rds_cluster.endpoint
  description = "The writer endpoint of the RDS cluster"
}

output "admin_db_login_secret_arn" {
  value       = aws_secretsmanager_secret.rds_admin_login.arn
  description = "The database admin login secret"
}

output "lambda_function_arns" {
  value = {
    for key, lambda in aws_lambda_function.gap_functions :
    key => lambda.arn
  }
  description = "ARNs of all gap Lambda functions"
}

