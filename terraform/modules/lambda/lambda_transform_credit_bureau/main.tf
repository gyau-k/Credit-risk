/*
 * Lambda Transform Credit Bureau - Terraform Configuration
 * Transforms raw credit bureau CSV to silver Parquet
 */

locals {
  function_name = "transform-credit-bureau-${var.environment}"

  common_tags = merge(
    var.tags,
    {
      Environment = var.environment
      ManagedBy   = "Terraform"
      Service     = "data-transformation"
      Dataset     = "credit-bureau"
    }
  )
}

# Data sources
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# Note: Lambda deployment package is pre-built using build-lambda.sh script
# This ensures all dependencies are properly bundled

# CloudWatch Log Group
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

# IAM Role for Lambda
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
resource "aws_iam_policy" "lambda_s3_policy" {
  name        = "${local.function_name}-s3-policy"
  description = "S3 permissions for credit bureau transformation Lambda"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadRawBucket"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion"
        ]
        Resource = [
          "arn:aws:s3:::${var.raw_bucket_name}/credit_bureau/raw/*"
        ]
      },
      {
        Sid    = "WriteSilverBucket"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:PutObjectAcl"
        ]
        Resource = [
          "arn:aws:s3:::${var.silver_bucket_name}/transformed/credit_bureau/*",
          "arn:aws:s3:::${var.silver_bucket_name}/_markers/*",
          "arn:aws:s3:::${var.silver_bucket_name}/error/credit_bureau/*"
        ]
      },
      {
        Sid    = "ListBuckets"
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.raw_bucket_name}",
          "arn:aws:s3:::${var.silver_bucket_name}"
        ]
      }
    ]
  })

  tags = local.common_tags
}

# IAM Policy for CloudWatch Logs
resource "aws_iam_policy" "lambda_cloudwatch_policy" {
  name        = "${local.function_name}-cloudwatch-policy"
  description = "CloudWatch Logs permissions for transformation Lambda"

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

# Attach policies to role
resource "aws_iam_role_policy_attachment" "lambda_s3_attach" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_cloudwatch_attach" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_cloudwatch_policy.arn
}

# Lambda Function
resource "aws_lambda_function" "transformer" {
  filename         = "${path.module}/../../../../src/lambda/lambda_transform_credit_bureau/dist/lambda_function.zip"
  function_name    = local.function_name
  role             = aws_iam_role.lambda_role.arn
  handler          = "lambda_function.lambda_handler"
  source_code_hash = filebase64sha256("${path.module}/../../../../src/lambda/lambda_transform_credit_bureau/dist/lambda_function.zip")
  runtime          = var.runtime
  timeout          = var.timeout
  memory_size      = var.memory_size

  # AWS SDK for pandas Layer includes pandas, numpy, pyarrow, and boto3
  layers = [
    "arn:aws:lambda:eu-west-1:336392948345:layer:AWSSDKPandas-Python311:23"
  ]

  # Environment variables
  environment {
    variables = {
      SILVER_BUCKET = var.silver_bucket_name
      LOG_LEVEL     = var.log_level
      ENVIRONMENT   = var.environment
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.lambda_logs,
    aws_iam_role_policy_attachment.lambda_s3_attach,
    aws_iam_role_policy_attachment.lambda_cloudwatch_attach
  ]

  tags = merge(
    local.common_tags,
    {
      Name = local.function_name
    }
  )
}

# Lambda permission to allow S3 to invoke
resource "aws_lambda_permission" "allow_s3_invoke" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.transformer.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = "arn:aws:s3:::${var.raw_bucket_name}"
}

# Note: S3 bucket notification is configured centrally in main.tf
# Multiple Lambda functions cannot each create their own notification on the same bucket

# CloudWatch Metric Alarm for errors
resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "${local.function_name}-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = "300"
  statistic           = "Sum"
  threshold           = "5"
  alarm_description   = "Alert when Lambda has more than 5 errors in 5 minutes"
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.transformer.function_name
  }

  tags = local.common_tags
}
