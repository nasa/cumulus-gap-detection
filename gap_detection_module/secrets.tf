resource "aws_secretsmanager_secret" "service_account_cert" {
  name  = "${var.DEPLOY_NAME}-gap-detection-service-account-cert"
  kms_key_id = data.aws_kms_key.secretsmanager.arn
}

resource "aws_secretsmanager_secret_version" "service_account_cert" {
  secret_id = aws_secretsmanager_secret.service_account_cert.id
  secret_string = jsonencode({
    cert       = "changeme"
    passphrase = "changeme"
  })
  
  lifecycle {
    ignore_changes = [secret_string]
  }
}
