# Lambda function for API polling 
# This module creates a Lambda function that polls an API endpoint on a schedule

# Data source for Lambda deployment package
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../../../../src/lambda/lambda_CT_script/build"
  output_path = "${path.module}/lambda_deployment.zip"
}

# Lambda Function
resource "aws_lambda_function" "api_poller" {
  filename         = data.archive_file.lambda_zip.output_path
  function_name    = var.function_name
  role             = aws_iam_role.lambda_execution.arn
  handler          = "api_poller_lambda.lambda_handler"
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  runtime          = var.runtime
  timeout          = var.timeout
  memory_size      = var.memory_size

  environment {
    variables = {
      API_ENDPOINT              = var.api_endpoint
      S3_BUCKET                 = var.s3_bucket_name
      S3_VALID_PREFIX           = var.s3_valid_prefix
      S3_ERROR_PREFIX           = var.s3_error_prefix
      REQUEST_TIMEOUT           = var.request_timeout
      MAX_TRANSACTIONS_PER_POLL = var.max_transactions_per_poll
      KMS_KEY_ID                = var.kms_key_id
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

# EventBridge (CloudWatch Events) Rule for scheduling
resource "aws_cloudwatch_event_rule" "api_poller_schedule" {
  name                = "${var.function_name}-schedule"
  description         = "Trigger ${var.function_name} on a schedule"
  schedule_expression = var.schedule_expression

  tags = {
    Name        = "${var.function_name}-schedule"
    Environment = var.environment
    Project     = var.project_name
  }
}

# EventBridge Target - Connect rule to Lambda
resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.api_poller_schedule.name
  target_id = "lambda"
  arn       = aws_lambda_function.api_poller.arn
}

# Lambda permission for EventBridge to invoke function
resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.api_poller.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.api_poller_schedule.arn
}

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

# IAM Policy for Lambda to write to S3
resource "aws_iam_role_policy" "lambda_s3_access" {
  name = "${var.function_name}-s3-access"
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
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.s3_bucket_name}",
          "arn:aws:s3:::${var.s3_bucket_name}/*"
        ]
      }
    ]
  })
}

# IAM Policy for Lambda to use KMS for encryption
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
