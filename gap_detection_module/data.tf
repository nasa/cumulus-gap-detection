# AWS provider

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# AWS-managed KMS key for secrets manager. Uses default KMS key id
data "aws_kms_key" "secretsmanager" {
  key_id = "alias/aws/secretsmanager"
}

