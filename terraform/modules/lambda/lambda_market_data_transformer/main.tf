# Lambda function for market data transformation
resource "aws_lambda_function" "market_data_transformer" {
  function_name = var.function_name
  role          = aws_iam_role.lambda_role.arn
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.11"
  timeout       = 900  # 15 minutes
  memory_size   = 1024

  filename         = data.archive_file.lambda_package.output_path
  source_code_hash = data.archive_file.lambda_package.output_base64sha256

  #  Add AWS SDK for pandas (Data Wrangler) Layer
  layers = [
    "arn:aws:lambda:eu-west-1:336392948345:layer:AWSSDKPandas-Python311:23"
  ]

  environment {
    variables = {
      RAW_BUCKET           = var.raw_bucket_name
      SILVER_BUCKET        = var.silver_bucket_name
      RAW_PREFIX           = var.raw_prefix
      CLEANSED_PREFIX      = var.cleansed_prefix
      REJECTED_PREFIX      = var.rejected_prefix
      PROCESSED_PREFIX     = var.processed_prefix
      DIM_MARKET_PREFIX    = var.dim_market_prefix
      BATCH_SIZE           = var.batch_size
      LOG_LEVEL            = var.log_level
      KMS_KEY_ID           = var.kms_key_id
    }
  }

  tags = {
    Name        = var.function_name
    Environment = var.environment
    Project     = var.project_name
  }
}

# Create deployment package
data "archive_file" "lambda_package" {
  type        = "zip"
  source_dir  = abspath("${path.module}/../../../../src/lambda/lambda_market_data_transformer/build")
  output_path = "${path.module}/market_data_transformer.zip"
}

# IAM role for Lambda
resource "aws_iam_role" "lambda_role" {
  name = "${var.function_name}-transformer-role"

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
    Name        = "${var.function_name}-role"
    Environment = var.environment
    Project     = var.project_name
  }
}

# IAM policy for Lambda
resource "aws_iam_role_policy" "lambda_policy" {
  name = "${var.function_name}-policy"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:CopyObject"
        ]
        Resource = [
          "${var.raw_bucket_arn}/*",
          "${var.silver_bucket_arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = [
          var.raw_bucket_arn,
          var.silver_bucket_arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:Encrypt",
          "kms:GenerateDataKey",
          "kms:GenerateDataKeyWithoutPlaintext",
          "kms:DescribeKey"
        ]
        Resource = [
          var.kms_key_arn
        ]
      }
    ]
  })
}

# CloudWatch Log Group
resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/${var.function_name}"
  retention_in_days = var.log_retention_days

  tags = {
    Name        = "${var.function_name}-logs"
    Environment = var.environment
    Project     = var.project_name
  }
}

# S3 Event Notification
# S3 bucket notification is configured centrally in terraform/envs/dev/main.tf
# Multiple Lambda functions cannot each create their own notification on the same bucket

# Lambda permission for S3 to invoke
resource "aws_lambda_permission" "allow_s3" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.market_data_transformer.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = var.raw_bucket_arn
}