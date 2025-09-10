resource "aws_sqs_queue" "gap_dlq" {
  name                    = "${var.DEPLOY_NAME}-gapDetectionIngestQueue-failed"
  sqs_managed_sse_enabled = true
}

resource "aws_sqs_queue" "gap_detection_ingest_queue" {
  name                       = "${var.DEPLOY_NAME}-gapDetectionIngestQueue"
  visibility_timeout_seconds = 60
  message_retention_seconds  = 1209600
  sqs_managed_sse_enabled    = true
  receive_wait_time_seconds  = 20
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.gap_dlq.arn
    maxReceiveCount     = 3
  })
}

# SQS policy to allow the SNS topic subscription
resource "aws_sqs_queue_policy" "allow_sns_subscription" {
  queue_url = aws_sqs_queue.gap_detection_ingest_queue.url
  policy = jsonencode(
    {
      "Version" : "2012-10-17",
      "Statement" : [
        {
          "Sid" : "__owner_statement",
          "Effect" : "Allow",
          "Principal" : {
            "AWS" : "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
          },
          "Action" : "SQS:*",
          "Resource" : aws_sqs_queue.gap_detection_ingest_queue.arn
        },
        {
          "Sid" : "AllowSNSPublish",
          "Effect" : "Allow",
          "Principal" : {
            "Service" : "sns.amazonaws.com"
          },
          "Action" : "SQS:SendMessage",
          "Resource" : aws_sqs_queue.gap_detection_ingest_queue.arn
          "Condition" : {
            "ArnEquals" : {
              "aws:SourceArn" : "${var.report_granules_topic_arn}"
            }
          }
        }
      ]
    }
  )
}
### Gap deletion resources

resource "aws_sqs_queue" "gap_deletion_dlq" {

  name = "${var.DEPLOY_NAME}-gapDetectionDeletionQueue-failed"

  sqs_managed_sse_enabled = true

}

resource "aws_sqs_queue" "gap_detection_deletion_queue" {

  name = "${var.DEPLOY_NAME}-gapDetectionDeletionQueue"

  visibility_timeout_seconds = 10

  message_retention_seconds = 1209600

  sqs_managed_sse_enabled = true

  receive_wait_time_seconds = 20

  redrive_policy = jsonencode({

    deadLetterTargetArn = aws_sqs_queue.gap_deletion_dlq.arn

    maxReceiveCount = 3

  })

}



resource "aws_sqs_queue_policy" "allow_sns_deletion_subscription" {

  queue_url = aws_sqs_queue.gap_detection_deletion_queue.url

  policy = jsonencode(

    {

      "Version" : "2012-10-17",

      "Statement" : [

        {

          "Sid" : "__owner_statement",

          "Effect" : "Allow",

          "Principal" : {

            "AWS" : "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"

          },

          "Action" : "SQS:*",

          "Resource" : aws_sqs_queue.gap_detection_deletion_queue.arn

        },

        {

          "Sid" : "AllowSNSPublish",

          "Effect" : "Allow",

          "Principal" : {

            "Service" : "sns.amazonaws.com"

          },

          "Action" : "SQS:SendMessage",

          "Resource" : aws_sqs_queue.gap_detection_deletion_queue.arn

          "Condition" : {

            "ArnEquals" : {

              "aws:SourceArn" : "${var.report_granules_topic_arn}"

            }

          }

        }

      ]

    }

  )
}
