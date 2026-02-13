resource "aws_secretsmanager_secret" "service_account_cert" {
  count = var.enable_authorizer ? 1 : 0
  name  = "${var.DEPLOY_NAME}-gap-detection-service-account-cert"
  kms_key_id = data.aws_kms_key.secretsmanager.arn
}

resource "aws_secretsmanager_secret_version" "service_account_cert" {
  count     = var.enable_authorizer ? 1 : 0
  secret_id = aws_secretsmanager_secret.service_account_cert[0].id
  secret_string = jsonencode({
    cert       = "changeme"
    passphrase = "changeme"
  })
  
  lifecycle {
    ignore_changes = [secret_string]
  }
}
