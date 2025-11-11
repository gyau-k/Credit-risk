####################################################
# Lambda Function: Check Glue Job Status
# Purpose: Check if a Glue job is currently running
####################################################

# CloudWatch Log Group
resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/${var.function_name}"
  retention_in_days = var.log_retention_days

  tags = merge(
    var.tags,
    {
      Name = "${var.function_name}-logs"
    }
  )
}

# IAM Role for Lambda
resource "aws_iam_role" "lambda_role" {
  name = "${var.function_name}-role"

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
    var.tags,
    {
      Name = "${var.function_name}-role"
    }
  )
}

# IAM Policy for CloudWatch Logs
resource "aws_iam_role_policy" "lambda_logs_policy" {
  name = "${var.function_name}-logs-policy"
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
        Resource = "${aws_cloudwatch_log_group.lambda_logs.arn}:*"
      }
    ]
  })
}

# IAM Policy for Glue Job Read Access
resource "aws_iam_role_policy" "glue_read_policy" {
  name = "${var.function_name}-glue-policy"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:GetJob",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:ListJobs"
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:job/${var.glue_job_name}",
          "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:job/*"
        ]
      }
    ]
  })
}

# Lambda Function
resource "aws_lambda_function" "check_glue_status" {
  filename         = "${var.lambda_source_dir}/lambda_check_glue_status.zip"
  function_name    = var.function_name
  role            = aws_iam_role.lambda_role.arn
  handler         = "lambda_function.lambda_handler"
  source_code_hash = filebase64sha256("${var.lambda_source_dir}/lambda_check_glue_status.zip")
  runtime         = var.runtime
  timeout         = var.timeout
  memory_size     = var.memory_size

  environment {
    variables = {
      GLUE_JOB_NAME = var.glue_job_name
      ENVIRONMENT   = var.environment
    }
  }

  lifecycle {
    ignore_changes = [source_code_hash]
  }

  depends_on = [
    aws_cloudwatch_log_group.lambda_logs,
    aws_iam_role_policy.lambda_logs_policy,
    aws_iam_role_policy.glue_read_policy
  ]

  tags = merge(
    var.tags,
    {
      Name = var.function_name
    }
  )
}
