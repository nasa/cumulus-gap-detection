resource "aws_iam_role" "gap_detection_eventbridge_scheduler_execution_role" {
  name = "${var.DEPLOY_NAME}-gap-detection-eventbridge-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "scheduler.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "gap_detection_eventbridge_scheduler_policy" {
  name = "${var.DEPLOY_NAME}-gap-detection-eventbridge-policy"
  role = aws_iam_role.gap_detection_eventbridge_scheduler_execution_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = "lambda:InvokeFunction"
        Resource = aws_lambda_function.gap_functions["gapReporter"].arn
      }
    ]
  })
}

resource "aws_scheduler_schedule" "gap_reporter_scheduler" {
  name                = "${var.DEPLOY_NAME}-gap_detection_reporter"
  group_name          = "default"
  schedule_expression = "rate(7 days)"
  
  flexible_time_window {
    mode = "OFF"
  }
  
  target {
    arn      = aws_lambda_function.gap_functions["gapReporter"].arn
    role_arn = aws_iam_role.gap_detection_eventbridge_scheduler_execution_role.arn
    
    # Optional: Add input if needed
    input = jsonencode({
      "source": "eventbridge-scheduler"
    })
  }
}

resource "aws_lambda_permission" "allow_scheduler_invoke" {
  statement_id  = "AllowSchedulerInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.gap_functions["gapReporter"].function_name
  principal     = "scheduler.amazonaws.com"
  source_arn    = aws_scheduler_schedule.gap_reporter_scheduler.arn
}