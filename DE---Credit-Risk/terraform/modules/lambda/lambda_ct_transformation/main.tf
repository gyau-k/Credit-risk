# Lambda function for Customer Transaction Transformation
# This module creates a Lambda function that transforms raw transaction data
# to silver layer with masking, validation, and Delta Lake storage

# S3 bucket for Lambda deployment packages
resource "aws_s3_bucket" "lambda_deployments" {
  bucket = "${var.function_name}-deployments"

  tags = {
    Name        = "${var.function_name}-deployments"
    Environment = var.environment
    Project     = var.project_name
  }
}

# Upload Lambda deployment package to S3
resource "aws_s3_object" "lambda_package" {
  bucket = aws_s3_bucket.lambda_deployments.id
  key    = "${var.function_name}/transformation_lambda.zip"
  source = "${path.module}/../../../../src/lambda/lambda_ct_transformation/dist/transformation_lambda.zip"
  etag   = filemd5("${path.module}/../../../../src/lambda/lambda_ct_transformation/dist/transformation_lambda.zip")

  tags = {
    Name        = "${var.function_name}-package"
    Environment = var.environment
    Project     = var.project_name
  }
}

# Lambda Function (deployed from S3 with AWS SDK for pandas Layer)
resource "aws_lambda_function" "transformation" {
  s3_bucket        = aws_s3_bucket.lambda_deployments.id
  s3_key           = aws_s3_object.lambda_package.key
  function_name    = var.function_name
  role             = aws_iam_role.lambda_execution.arn
  handler          = "transformation_lambda.lambda_handler"
  source_code_hash = filebase64sha256("${path.module}/../../../../src/lambda/lambda_ct_transformation/dist/transformation_lambda.zip")
  runtime          = var.runtime
  timeout          = var.timeout
  memory_size      = var.memory_size

  # AWS SDK for pandas Layer includes pandas, numpy, pyarrow, and boto3
  layers = [
    "arn:aws:lambda:eu-west-1:336392948345:layer:AWSSDKPandas-Python311:23"
  ]

  environment {
    variables = {
      S3_RAW_BUCKET         = var.s3_raw_bucket
      S3_SILVER_BUCKET      = var.s3_silver_bucket
      S3_TRANSFORMED_PREFIX = var.s3_transformed_prefix
      S3_FACT_PREFIX        = var.s3_fact_prefix
      S3_ERROR_PREFIX       = var.s3_error_prefix
      TOKENIZATION_SALT     = var.tokenization_salt
      DELTA_WRITE_MODE      = var.delta_write_mode
      KMS_KEY_ID            = var.kms_key_id
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.lambda_logs,
    aws_iam_role_policy_attachment.lambda_basic_execution
  ]

  tags = {
    Name        = var.function_name
    Environment = var.environment
    Project     = var.project_name
  }
}

# CloudWatch Log Group for Lambda
resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/${var.function_name}"
  retention_in_days = var.log_retention_days

  tags = {
    Name        = "${var.function_name}-logs"
    Environment = var.environment
    Project     = var.project_name
  }
}

# S3 Event Notification Permission
resource "aws_lambda_permission" "allow_s3" {
  statement_id  = "AllowExecutionFromS3"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.transformation.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = "arn:aws:s3:::${var.s3_raw_bucket}"
}

# S3 Bucket Notification
# NOTE: S3 bucket notification is configured centrally in terraform/envs/dev/main.tf
# Multiple Lambda functions cannot each create their own notification on the same bucket

# IAM Role for Lambda execution
resource "aws_iam_role" "lambda_execution" {
  name = "${var.function_name}-execution-role"

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

  tags = {
    Name        = "${var.function_name}-execution-role"
    Environment = var.environment
    Project     = var.project_name
  }
}

# IAM Policy for Lambda to write to CloudWatch Logs
resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  role       = aws_iam_role.lambda_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# IAM Policy for Lambda to read from raw S3 bucket
resource "aws_iam_role_policy" "lambda_s3_raw_read" {
  name = "${var.function_name}-s3-raw-read"
  role = aws_iam_role.lambda_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:HeadObject"
        ]
        Resource = [
          "arn:aws:s3:::${var.s3_raw_bucket}",
          "arn:aws:s3:::${var.s3_raw_bucket}/*"
        ]
      }
    ]
  })
}

# IAM Policy for Lambda to write to silver S3 bucket
resource "aws_iam_role_policy" "lambda_s3_silver_write" {
  name = "${var.function_name}-s3-silver-write"
  role = aws_iam_role.lambda_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:PutObjectAcl",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:DeleteObject"
        ]
        Resource = [
          "arn:aws:s3:::${var.s3_silver_bucket}",
          "arn:aws:s3:::${var.s3_silver_bucket}/*"
        ]
      }
    ]
  })
}

# IAM Policy for Lambda to use KMS for encryption/decryption
resource "aws_iam_role_policy" "lambda_kms_access" {
  name = "${var.function_name}-kms-access"
  role = aws_iam_role.lambda_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:Encrypt",
          "kms:GenerateDataKey",
          "kms:DescribeKey"
        ]
        Resource = [
          var.kms_key_arn
        ]
      }
    ]
  })
}
