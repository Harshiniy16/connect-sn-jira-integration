# Skeleton only: shows structure and the resources that matter for correctness.
terraform {
  required_version = ">= 1.6"
  backend "s3" {}        # bucket/key/dynamodb_table supplied per environment
}

variable "env" { type = string }

resource "aws_dynamodb_table" "idempotency" {
  name         = "cc-int-${var.env}-idempotency"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "interaction_key"

  attribute {
    name = "interaction_key"
    type = "S"
  }
  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }
  point_in_time_recovery { enabled = true }
  lifecycle { prevent_destroy = true }
}

resource "aws_sqs_queue" "sn_write_dlq" {
  name                      = "cc-int-${var.env}-sn-write-dlq"
  message_retention_seconds = 1209600
}

resource "aws_sqs_queue" "sn_write" {
  name                       = "cc-int-${var.env}-sn-write"
  visibility_timeout_seconds = 180        # >= 6x Lambda timeout
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.sn_write_dlq.arn
    maxReceiveCount     = 8
  })
}

resource "aws_kinesis_stream" "ctr" {
  name             = "cc-int-${var.env}-ctr"
  retention_period = 168
  stream_mode_details { stream_mode = "ON_DEMAND" }
}

resource "aws_lambda_function" "sn_writer" {
  function_name                  = "cc-int-${var.env}-sn-writer"
  runtime                        = "python3.12"
  handler                        = "lambdas.sn_writer.handler"
  reserved_concurrent_executions = 5      # protects ServiceNow rate limits
  timeout                        = 30
  # role, code, env vars omitted
}

resource "aws_cloudwatch_metric_alarm" "dlq_not_empty" {
  alarm_name          = "cc-int-${var.env}-sn-write-dlq-depth"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.sn_write_dlq.name }
  statistic           = "Maximum"
  comparison_operator = "GreaterThanThreshold"
  threshold           = 0
  period              = 60
  evaluation_periods  = 1
}
