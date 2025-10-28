## =============================================================================
## Networking resources
## =============================================================================
resource "aws_db_subnet_group" "rds_subnet_group" {
  name_prefix = "${var.DEPLOY_NAME}-gap-detection"
  subnet_ids  = var.subnet_ids
}

resource "aws_security_group" "rds_cluster_security_group" {
  name_prefix = "${var.DEPLOY_NAME}-gap-detection"
  vpc_id      = var.vpc_id
}

# Need this to allow RDS proxy connection to RDS cluster instance(s)
resource "aws_security_group_rule" "rds_security_group_allow_postgres" {
  type                     = "ingress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  security_group_id        = aws_security_group.rds_cluster_security_group.id
  source_security_group_id = aws_security_group.rds_proxy_sg.id
}
# Adds outbound rule to allow all traffic for rds proxy security group
resource "aws_security_group_rule" "allow_all_outbound" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"          # "-1" means all protocols
  cidr_blocks       = ["0.0.0.0/0"] # Allows traffic to any IP address
  security_group_id = aws_security_group.rds_proxy_sg.id
}


## =============================================================================
## Secret manager resources
## =============================================================================
resource "aws_secretsmanager_secret" "rds_admin_login" {
  name = "${var.DEPLOY_NAME}-gap-detection-rds-db-admin-credentials"
}

resource "aws_secretsmanager_secret_version" "rds_admin_login" {
  secret_id = aws_secretsmanager_secret.rds_admin_login.id
  secret_string = jsonencode({
    username            = var.db_admin_username
    password            = random_string.admin_db_pass.result
    database            = "postgres"
    engine              = "aurora-postgresql"
    host                = aws_rds_cluster.rds_cluster.endpoint
    port                = 5432
    dbClusterIdentifier = aws_rds_cluster.rds_cluster.id
    disableSSL          = true
  })
}

resource "aws_secretsmanager_secret" "rds_user_login" {
  name = "${var.DEPLOY_NAME}-gap-detection-rds-db-user-credentials"
}

resource "aws_secretsmanager_secret_version" "rds_user_login" {
  secret_id = aws_secretsmanager_secret.rds_user_login.id
  secret_string = jsonencode({
    username            = replace(var.DEPLOY_NAME, "-", "_")
    password            = random_string.user_db_pass.result
    database            = "postgres"
    engine              = "aurora-postgresql"
    host                = aws_rds_cluster.rds_cluster.endpoint
    port                = 5432
    dbClusterIdentifier = aws_rds_cluster.rds_cluster.id
    disableSSL          = true
  })
}

# Creates random admin password which can be accessed from secrets manager
resource "random_string" "admin_db_pass" {
  length  = 50
  upper   = true
  special = false
}

# Creates random user password which can be accessed from secrets manager
resource "random_string" "user_db_pass" {
  length  = 50
  upper   = true
  special = false
}

## =============================================================================
## Database resources
## =============================================================================
resource "aws_rds_cluster" "rds_cluster" {
  depends_on                      = [aws_db_subnet_group.rds_subnet_group]
  cluster_identifier              = "${var.DEPLOY_NAME}-gap-detection-rds-cluster"
  engine                          = "aurora-postgresql"
  engine_version                  = var.engine_version
  database_name                   = "postgres"
  master_username                 = var.db_admin_username
  master_password                 = random_string.admin_db_pass.result
  preferred_backup_window         = var.backup_window
  performance_insights_enabled    = true
  db_subnet_group_name            = aws_db_subnet_group.rds_subnet_group.id
  apply_immediately               = var.apply_immediately
  vpc_security_group_ids          = [aws_security_group.rds_cluster_security_group.id]
  deletion_protection             = var.deletion_protection
  final_snapshot_identifier       = "${var.DEPLOY_NAME}-gap-detection-final-snapshot"
  snapshot_identifier             = var.snapshot_identifier
  storage_encrypted               = true
  enabled_cloudwatch_logs_exports = ["postgresql"]
  enable_http_endpoint            = true

  serverlessv2_scaling_configuration {
    min_capacity = 2.0
    max_capacity = 16.0
  }
}

resource "aws_rds_cluster_instance" "cluster_instances" {
  cluster_identifier         = aws_rds_cluster.rds_cluster.id
  auto_minor_version_upgrade = false
  instance_class             = "db.serverless"
  engine                     = aws_rds_cluster.rds_cluster.engine
  engine_version             = aws_rds_cluster.rds_cluster.engine_version
}

## =============================================================================
## Database proxy resources
## =============================================================================

# RDS Proxy security group
resource "aws_security_group" "rds_proxy_sg" {
  name        = "${var.DEPLOY_NAME}-rds-proxy-sg"
  description = "Security group for RDS Proxy"
  vpc_id      = var.vpc_id
}

# Security group rule for RDS Proxy allowing inbound traffic from Lambda
resource "aws_security_group_rule" "proxy_from_lambda" {
  type                     = "ingress"
  from_port                = 5432 # MySQL port (5432 for PostgreSQL)
  to_port                  = 5432
  protocol                 = "tcp"
  source_security_group_id = var.security_group_id
  security_group_id        = aws_security_group.rds_proxy_sg.id
}

# IAM role for the proxy
resource "aws_iam_role" "rds_proxy_role" {
  name = "${var.DEPLOY_NAME}-gap-detection-rds-proxy-role"
  assume_role_policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Principal" : {
          "Service" : "rds.amazonaws.com"
        },
        "Action" : "sts:AssumeRole"
      }
    ]
  })
}

# Attach permissions to RDS proxy to read/decrypt RDS secret and connect to RDS cluster
resource "aws_iam_role_policy" "rds_proxy_role_policy" {
  name = "${var.DEPLOY_NAME}-gap-detection-rds-proxy-policy"
  role = aws_iam_role.rds_proxy_role.id
  policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Sid" : "GetSecretValue",
        "Action" : [
          "secretsmanager:GetSecretValue"
        ],
        "Effect" : "Allow",
        "Resource" : [
          aws_secretsmanager_secret.rds_admin_login.arn
        ]
      },
      {
        "Sid" : "DecryptSecretValue",
        "Action" : [
          "kms:Decrypt"
        ],
        "Effect" : "Allow",
        "Resource" : [
          data.aws_kms_key.secretsmanager.arn
        ],
        "Condition" : {
          "StringEquals" : {
            "kms:ViaService" : "secretsmanager.us-west-2.amazonaws.com"
          }
        }
      },
      {
        Sid    = "AllowRDSConnection"
        Effect = "Allow"
        Action = [
          "rds-db:connect"
        ]
        Resource = "arn:aws:rds-db:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:dbuser:${aws_rds_cluster.rds_cluster.cluster_resource_id}/*"
      }
    ]
  })
}

# RDS DB proxy
resource "aws_db_proxy" "rds_proxy" {
  name                   = "${var.DEPLOY_NAME}-gap-detection-proxy"
  engine_family          = "POSTGRESQL"
  role_arn               = aws_iam_role.rds_proxy_role.arn
  vpc_security_group_ids = [aws_security_group.rds_proxy_sg.id]
  vpc_subnet_ids         = var.subnet_ids

  auth {
    auth_scheme               = "SECRETS"
    iam_auth                  = "DISABLED"
    client_password_auth_type = "POSTGRES_SCRAM_SHA_256"
    secret_arn                = aws_secretsmanager_secret.rds_admin_login.arn
  }
}

# RDS proxy default target group (for connection config)
resource "aws_db_proxy_default_target_group" "defualt_proxy_target_group" {
  db_proxy_name = aws_db_proxy.rds_proxy.name
  connection_pool_config {
    connection_borrow_timeout    = 10
    max_connections_percent      = 100
    max_idle_connections_percent = 50
  }
}

# RDS DB proxy target group
resource "aws_db_proxy_default_target_group" "rds_proxy_target_group" {
  db_proxy_name = aws_db_proxy.rds_proxy.name
}

# RDS DB proxy target
resource "aws_db_proxy_target" "rds_proxy_target" {
  db_cluster_identifier = aws_rds_cluster.rds_cluster.cluster_identifier
  db_proxy_name         = aws_db_proxy.rds_proxy.name
  target_group_name     = aws_db_proxy_default_target_group.rds_proxy_target_group.name
}
