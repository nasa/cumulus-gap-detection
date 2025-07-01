# S3 bucket for gap reports
resource "aws_s3_bucket" "gap_report_bucket" {
  bucket = "${var.DEPLOY_NAME}-gap-reports"
}
resource "aws_s3_bucket" "artifacts_bucket" {
  bucket = "${var.DEPLOY_NAME}-gap-detection-artifacts-bucket"

}
# S3 bucket policy for Lambda functions to access gap report S3 bucket
resource "aws_iam_role_policy" "gap_report_s3_access_policy" {
  name = "${var.DEPLOY_NAME}-gap-report-s3-access"
  role = var.lambda_processing_role_name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:DeleteObject"
        ]
        Resource = [
          aws_s3_bucket.gap_report_bucket.arn,
          "${aws_s3_bucket.gap_report_bucket.arn}/*"
        ]
      }
    ]
  })
}

# S3 bucket for storing large results from getTimeGaps query
resource "aws_s3_bucket" "gap_response_bucket" {
  bucket = "${var.DEPLOY_NAME}-gap-response"
}

# S3 bucket policy for Lambda functions to access gap report S3 bucket
resource "aws_iam_role_policy" "gap_response_s3_access_policy" {
  name = "${var.DEPLOY_NAME}-gap-response-s3-access"
  role = var.lambda_processing_role_name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:DeleteObject"
        ]
        Resource = [
          aws_s3_bucket.gap_response_bucket.arn,
          "${aws_s3_bucket.gap_response_bucket.arn}/*"
        ]
      }
    ]
  })
}
