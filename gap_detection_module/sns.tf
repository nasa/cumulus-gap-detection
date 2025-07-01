resource "aws_sns_topic" "gap_migration_stream" {
  name = "${var.DEPLOY_NAME}-gap_migration_stream"
}

resource "aws_sns_topic_subscription" "gap_detection_sqs_target" {
  topic_arn  = aws_sns_topic.gap_migration_stream.arn
  protocol   = "sqs"
  endpoint   = aws_sqs_queue.gap_detection_ingest_queue.arn
  depends_on = [aws_sqs_queue_policy.allow_sns_subscription]
}

resource "aws_sns_topic_subscription" "gap_detection_lambda_trigger" {
  topic_arn  = aws_sns_topic.gap_migration_stream.arn
  protocol   = "lambda"
  endpoint   = aws_lambda_function.gap_functions["gapMigrationStreamMessageCompiler"].arn
  depends_on = [aws_sqs_queue_policy.allow_sns_subscription]
}

# Trigger lambda from SNS topic
resource "aws_lambda_permission" "sns_lambda_permission" {
  statement_id  = "AllowExecutionFromSNS"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.gap_functions["gapMigrationStreamMessageCompiler"].function_name
  principal     = "sns.amazonaws.com"
  source_arn    = aws_sns_topic.gap_migration_stream.arn
}


resource "aws_sns_topic_subscription" "report_granules_deletion_subscription" {

  topic_arn = var.report_granules_topic_arn

  protocol = "sqs"

  endpoint = aws_sqs_queue.gap_detection_deletion_queue.arn

  filter_policy = jsonencode({ placeholder : ["placeholder"] })

  filter_policy_scope = "MessageBody"

  lifecycle {

    ignore_changes = [filter_policy]

  }

}



# Create an SNS subscription with a filter policy

resource "aws_sns_topic_subscription" "report_granules_ingest_subscription" {

  topic_arn = var.report_granules_topic_arn

  protocol = "sqs"

  endpoint = aws_sqs_queue.gap_detection_ingest_queue.arn

  filter_policy = jsonencode({ placeholder : ["placeholder"] })

  filter_policy_scope = "MessageBody"

  lifecycle {

    ignore_changes = [filter_policy]

  }

}
