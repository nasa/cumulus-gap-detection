
output "DEPLOY_NAME" {
  description = "Deployment name used as a prefix for gap detection resources"
  value       = var.DEPLOY_NAME
}
output "vpc_id" {
  description = ""
  value       = var.vpc_id
}

# output "child_secretsmanager" {
#    value = data.aws_kms_key.secretsmanager
# }
