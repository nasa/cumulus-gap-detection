resource "aws_dynamodb_table" "tolerance_table" {
  name         = "${var.DEPLOY_NAME}-gap-detection-tolerance-table"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "shortname"
  range_key    = "versionid"
  attribute {
    name = "shortname"
    type = "S"
  }
  attribute {
    name = "versionid"
    type = "S"
  }
}

# Inline policy being attached to lambda processing role which allows lambdas to use table
resource "aws_iam_role_policy" "lambda_inline_policy" {
  name = "${var.DEPLOY_NAME}-lambda-inline-policy"
  role = var.lambda_processing_role_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:Query"
        ]
        Resource = aws_dynamodb_table.tolerance_table.arn
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}
