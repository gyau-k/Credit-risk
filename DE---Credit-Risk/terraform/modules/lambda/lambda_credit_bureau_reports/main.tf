/*
 * CSV Header Validation Lambda - Terraform Configuration
 * 
 * This Terraform configuration sets up:
 * - Lambda function with proper IAM permissions
 * - CloudWatch log group for observability
 * - IAM roles and policies following least-privilege principle
 * 
 * Design Decisions:
 * - Separate buckets for landing vs processed data (security boundary)
 * - Lambda reserved concurrent executions to control costs
 * - CloudWatch log retention for compliance and cost management
 * - Environment variables for configuration flexibility
 */


# Local variables for computed values
locals {
  function_name = "credit-bureau-validator-${var.environment}"

  # Construct full paths for environment variables
  source_path = "${var.landing_bucket_name}/${var.landing_prefix}"
  raw_path    = "${var.processed_bucket_name}/${var.raw_prefix}"
  reject_path = "${var.processed_bucket_name}/${var.reject_prefix}"

  common_tags = merge(
    var.tags,
    {
      Environment = var.environment
      ManagedBy   = "Terraform"
      Service     = "csv-validation"
    }
  )
}

# Data source to get current AWS account ID and region
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# Archive Lambda function code into deployment package
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/../../../../src/lambda/lambda_credit_bureau_script/lambda_function.py"
  output_path = "${path.module}/lambda_function.zip"
}

# CloudWatch Log Group for Lambda function logs
# Created before Lambda to prevent auto-creation with infinite retention
resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/${local.function_name}"
  retention_in_days = var.log_retention_days

  tags = merge(
    local.common_tags,
    {
      Name = "${local.function_name}-logs"
    }
  )
}

# IAM Role for Lambda function
resource "aws_iam_role" "lambda_role" {
  name = "${local.function_name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })

  tags = merge(
    local.common_tags,
    {
      Name = "${local.function_name}-role"
    }
  )
}


# IAM Policy for S3 access
# Follows least-privilege: read from landing, read/write to processed bucket
resource "aws_iam_policy" "lambda_s3_policy" {
  name        = "${local.function_name}-s3-policy"
  description = "S3 permissions for CSV validation Lambda"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # Read access to landing bucket
        Sid    = "ReadLandingBucket"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion"
        ]
        Resource = [
          "arn:aws:s3:::${var.landing_bucket_name}/${var.landing_prefix}*"
        ]
      },
      {
        # Delete access to landing bucket (for move operation)
        Sid    = "DeleteFromLandingBucket"
        Effect = "Allow"
        Action = [
          "s3:DeleteObject"
        ]
        Resource = [
          "arn:aws:s3:::${var.landing_bucket_name}/${var.landing_prefix}*"
        ]
      },
      {
        # Write access to processed bucket (raw and rejected paths)
        Sid    = "WriteToProcessedBucket"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:PutObjectAcl"
        ]
        Resource = [
          "arn:aws:s3:::${var.processed_bucket_name}/${var.raw_prefix}*",
          "arn:aws:s3:::${var.processed_bucket_name}/${var.reject_prefix}*"
        ]
      },
      {
        # List bucket permissions (needed for copy operations)
        Sid    = "ListBuckets"
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.landing_bucket_name}",
          "arn:aws:s3:::${var.processed_bucket_name}"
        ]
      }
    ]
  })

  tags = local.common_tags
}

# IAM Policy for CloudWatch Logs
resource "aws_iam_policy" "lambda_cloudwatch_policy" {
  name        = "${local.function_name}-cloudwatch-policy"
  description = "CloudWatch Logs permissions for CSV validation Lambda"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = [
          "${aws_cloudwatch_log_group.lambda_logs.arn}:*"
        ]
      }
    ]
  })

  tags = local.common_tags
}

# IAM Policy for KMS encryption
resource "aws_iam_policy" "lambda_kms_policy" {
  name        = "${local.function_name}-kms-policy"
  description = "KMS permissions for S3 encryption"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:GenerateDataKey",
          "kms:DescribeKey"
        ]
        Resource = [
          var.kms_key_arn
        ]
      }
    ]
  })

  tags = local.common_tags
}

# Attach S3 policy to Lambda role
resource "aws_iam_role_policy_attachment" "lambda_s3_attach" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_s3_policy.arn
}

# Attach CloudWatch policy to Lambda role
resource "aws_iam_role_policy_attachment" "lambda_cloudwatch_attach" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_cloudwatch_policy.arn
}

# Attach KMS policy to Lambda role
resource "aws_iam_role_policy_attachment" "lambda_kms_attach" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_kms_policy.arn
}

# Lambda Function
resource "aws_lambda_function" "csv_validator" {
  filename         = data.archive_file.lambda_zip.output_path
  function_name    = local.function_name
  role             = aws_iam_role.lambda_role.arn
  handler          = "lambda_function.lambda_handler"
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  runtime          = "python3.11"
  timeout          = var.lambda_timeout
  memory_size      = var.lambda_memory_size

  # Reserved concurrent executions for cost control
  reserved_concurrent_executions = var.reserved_concurrent_executions

  # Environment variables for configuration
  environment {
    variables = {
      SOURCE_PATH = local.source_path
      RAW_PATH    = local.raw_path
      REJECT_PATH = local.reject_path
      LOG_LEVEL   = var.log_level
      ENVIRONMENT = var.environment
      KMS_KEY_ID  = var.kms_key_id
    }
  }

  # Explicit dependency on CloudWatch log group
  depends_on = [
    aws_cloudwatch_log_group.lambda_logs,
    aws_iam_role_policy_attachment.lambda_s3_attach,
    aws_iam_role_policy_attachment.lambda_cloudwatch_attach,
    aws_iam_role_policy_attachment.lambda_kms_attach
  ]

  tags = merge(
    local.common_tags,
    {
      Name = local.function_name
    }
  )
}

# # Lambda permission to allow S3 to invoke the function
# resource "aws_lambda_permission" "allow_s3_invoke" {
#   statement_id  = "AllowS3Invoke"
#   action        = "lambda:InvokeFunction"
#   function_name = aws_lambda_function.csv_validator.function_name
#   principal     = "s3.amazonaws.com"
#   source_arn    = "arn:aws:s3:::${var.landing_bucket_name}"
# }

# # S3 bucket notification to trigger Lambda
# # This configures the landing bucket to trigger Lambda on new CSV files
# resource "aws_s3_bucket_notification" "landing_bucket_notification" {
#   bucket = var.landing_bucket_name

#   lambda_function {
#     lambda_function_arn = aws_lambda_function.csv_validator.arn
#     events              = ["s3:ObjectCreated:*"]
#     filter_prefix       = var.landing_prefix
#     filter_suffix       = ".csv"
#   }

#   depends_on = [aws_lambda_permission.allow_s3_invoke]
# }

# CloudWatch Metric Alarm for Lambda errors
resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "${local.function_name}-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = "300"
  statistic           = "Sum"
  threshold           = "5"
  alarm_description   = "Alert when Lambda function has more than 5 errors in 5 minutes"
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.csv_validator.function_name
  }

  tags = local.common_tags
}

# CloudWatch Metric Alarm for Lambda throttles
resource "aws_cloudwatch_metric_alarm" "lambda_throttles" {
  alarm_name          = "${local.function_name}-throttles"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "Throttles"
  namespace           = "AWS/Lambda"
  period              = "300"
  statistic           = "Sum"
  threshold           = "1"
  alarm_description   = "Alert when Lambda function is throttled"
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.csv_validator.function_name
  }

  tags = local.common_tags
}

