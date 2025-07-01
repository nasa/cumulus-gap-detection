terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}


# TODO Parameterize from vars
locals {

  region        = data.aws_region.current.name
  account_id    = data.aws_caller_identity.current.account_id
  base_url      = "https://console.aws.amazon.com/states/home"
  execution_url = "${local.base_url}?region=${local.region}#/executions/details"

  state_machines = [
    data.aws_sfn_state_machine.component_metadata_state_machine.name,
    data.aws_sfn_state_machine.component_cmr_state_machine.name
  ]
  gap_functions = {
    gapUpdate = {
      timeout     = 10
      memory_size = 512
      variables = {
        RDS_SECRET         = aws_secretsmanager_secret.rds_admin_login.name
        RDS_PROXY_HOST     = aws_db_proxy.rds_proxy.endpoint
        CMR_ENV            = "PROD"
        DELETION_QUEUE_ARN = aws_sqs_queue.gap_detection_deletion_queue.arn
      }
    }
    gapMigrationStreamMessageCompiler = {
      timeout     = 900
      memory_size = 2048
      variables = {
        QUEUE_URL = aws_sqs_queue.gap_detection_ingest_queue.url
      }
    }
    gapReporter = {
      timeout = 900
      variables = {
        RDS_SECRET        = aws_secretsmanager_secret.rds_admin_login.name
        RDS_PROXY_HOST    = aws_db_proxy.rds_proxy.endpoint
        TOLERANCE_TABLE   = aws_dynamodb_table.tolerance_table.name
        GAP_REPORT_BUCKET = aws_s3_bucket.gap_report_bucket.id
      }
    }
    gapCreateTable = {
      variables = {
        RDS_SECRET     = aws_secretsmanager_secret.rds_admin_login.name
        RDS_PROXY_HOST = aws_db_proxy.rds_proxy.endpoint
      }
    }
    knownGap = {
      is_api_handler = true
      variables = {
        RDS_SECRET     = aws_secretsmanager_secret.rds_admin_login.name
        RDS_PROXY_HOST = aws_db_proxy.rds_proxy.endpoint
      }
    }
    gapConfig = {
      is_api_handler = true
      timeout        = 900
      variables = {
        RDS_SECRET                       = aws_secretsmanager_secret.rds_admin_login.name
        RDS_PROXY_HOST                   = aws_db_proxy.rds_proxy.endpoint
        CMR_ENV                          = "PROD"
        MIGRATION_STREAM_COMPILER_LAMBDA = "${var.DEPLOY_NAME}-gapMigrationStreamMessageCompiler"
        TOLERANCE_TABLE_NAME             = aws_dynamodb_table.tolerance_table.name
        EXECUTION_ARN_PREFIX_INGEST = jsonencode([
          for name in local.state_machines :
          "${local.execution_url}/arn:aws:states:${local.region}:${local.account_id}:execution:${name}"

        ])
        SUBSCRIPTION_ARN_DELETION = aws_sns_topic_subscription.report_granules_deletion_subscription.arn
        SUBSCRIPTION_ARN_INGEST   = aws_sns_topic_subscription.report_granules_ingest_subscription.arn
      }
    }
    getTimeGaps = {
      is_api_handler = true
      timeout        = 30
      memory_size    = 256
      variables = {
        RDS_SECRET          = aws_secretsmanager_secret.rds_admin_login.name
        RDS_PROXY_HOST      = aws_db_proxy.rds_proxy.endpoint
        CMR_ENV             = "PROD"
        TOLERANCE_TABLE     = aws_dynamodb_table.tolerance_table.name
        GAP_RESPONSE_BUCKET = aws_s3_bucket.gap_response_bucket.id
      }
    }
    getGapReport = {
      is_api_handler = true
      variables = {
        GAP_REPORT_BUCKET = aws_s3_bucket.gap_report_bucket.id
      }
    }
  }

  # Filter functions that have dependency archives
  functions_with_deps = {
    for k, v in local.gap_functions : k => v
    if fileexists("artifacts/layers/${k}-deps.zip")
  }

  # List of functions that need API Gateway permissions
  api_functions = [
    for name, config in local.gap_functions : name
    if lookup(config, "is_api_handler", false)
  ]

  openapi_template_vars = {
    DEPLOY_NAME = var.DEPLOY_NAME
    lambda_invoke_arns = {
      for func_name in local.api_functions :
      func_name => aws_lambda_function.gap_functions[func_name].invoke_arn
    }
  }
}
