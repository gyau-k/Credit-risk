####################################################
# Step Functions: Credit Risk Transformation Workflow
# Purpose: Orchestrate Bronze->Silver->Gold transformations
####################################################

# CloudWatch Log Group for Step Function
resource "aws_cloudwatch_log_group" "sfn_logs" {
  name              = "/aws/states/${var.state_machine_name}"
  retention_in_days = var.log_retention_days

  tags = merge(
    var.tags,
    {
      Name = "${var.state_machine_name}-logs"
    }
  )
}

# SNS Topic for Failure Notifications
resource "aws_sns_topic" "workflow_failures" {
  name = "${var.state_machine_name}-failures"

  tags = merge(
    var.tags,
    {
      Name = "${var.state_machine_name}-failures"
    }
  )
}

# SNS Topic Subscription (Email)
resource "aws_sns_topic_subscription" "workflow_failures_email" {
  count     = length(var.notification_emails)
  topic_arn = aws_sns_topic.workflow_failures.arn
  protocol  = "email"
  endpoint  = var.notification_emails[count.index]
}

# IAM Role for Step Function
resource "aws_iam_role" "sfn_role" {
  name = "${var.state_machine_name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = merge(
    var.tags,
    {
      Name = "${var.state_machine_name}-role"
    }
  )
}

# IAM Policy for Lambda Invocation
resource "aws_iam_role_policy" "lambda_invoke_policy" {
  name = "${var.state_machine_name}-lambda-policy"
  role = aws_iam_role.sfn_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = [
          var.lambda_transform_loan_applications_arn,
          var.lambda_transform_credit_bureau_arn,
          var.lambda_transform_loan_repayments_arn,
          var.lambda_check_glue_status_arn
        ]
      }
    ]
  })
}

# IAM Policy for Glue Job Execution
resource "aws_iam_role_policy" "glue_execution_policy" {
  name = "${var.state_machine_name}-glue-policy"
  role = aws_iam_role.sfn_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun"
        ]
        Resource = "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:job/${var.glue_job_name}"
      }
    ]
  })
}

# IAM Policy for SNS Publishing
resource "aws_iam_role_policy" "sns_publish_policy" {
  name = "${var.state_machine_name}-sns-policy"
  role = aws_iam_role.sfn_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "sns:Publish"
        ]
        Resource = aws_sns_topic.workflow_failures.arn
      }
    ]
  })
}

# IAM Policy for S3 Error Logging
resource "aws_iam_role_policy" "s3_error_logging_policy" {
  name = "${var.state_machine_name}-s3-policy"
  role = aws_iam_role.sfn_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:PutObjectAcl"
        ]
        Resource = "arn:aws:s3:::${var.silver_bucket_name}/error/*"
      }
    ]
  })
}

# IAM Policy for CloudWatch Logs
resource "aws_iam_role_policy" "cloudwatch_logs_policy" {
  name = "${var.state_machine_name}-logs-policy"
  role = aws_iam_role.sfn_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogDelivery",
          "logs:GetLogDelivery",
          "logs:UpdateLogDelivery",
          "logs:DeleteLogDelivery",
          "logs:ListLogDeliveries",
          "logs:PutLogEvents",
          "logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies",
          "logs:DescribeLogGroups"
        ]
        Resource = "*"
      }
    ]
  })
}

# Step Function State Machine
resource "aws_sfn_state_machine" "credit_risk_workflow" {
  name     = var.state_machine_name
  role_arn = aws_iam_role.sfn_role.arn

  definition = templatefile("${path.module}/state_machine.asl.json", {
    lambda_transform_loan_applications_arn = var.lambda_transform_loan_applications_arn
    lambda_transform_credit_bureau_arn     = var.lambda_transform_credit_bureau_arn
    lambda_transform_loan_repayments_arn   = var.lambda_transform_loan_repayments_arn
    lambda_check_glue_status_arn           = var.lambda_check_glue_status_arn
    glue_job_name                          = var.glue_job_name
    silver_bucket_name                     = var.silver_bucket_name
    sns_topic_arn                          = aws_sns_topic.workflow_failures.arn
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn_logs.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }

  tracing_configuration {
    enabled = var.enable_xray_tracing
  }

  tags = merge(
    var.tags,
    {
      Name = var.state_machine_name
    }
  )
}

####################################################
# EventBridge Rules to Trigger Step Function
####################################################

# IAM Role for EventBridge to invoke Step Function
resource "aws_iam_role" "eventbridge_sfn_role" {
  name = "${var.state_machine_name}-eventbridge-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = merge(
    var.tags,
    {
      Name = "${var.state_machine_name}-eventbridge-role"
    }
  )
}

# IAM Policy for EventBridge to Start Step Function
resource "aws_iam_role_policy" "eventbridge_sfn_policy" {
  name = "${var.state_machine_name}-eventbridge-policy"
  role = aws_iam_role.eventbridge_sfn_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "states:StartExecution"
        ]
        Resource = aws_sfn_state_machine.credit_risk_workflow.arn
      }
    ]
  })
}

# EventBridge Rule: Loan Applications
resource "aws_cloudwatch_event_rule" "loan_applications_trigger" {
  name        = "${var.state_machine_name}-loan-apps-trigger"
  description = "Trigger Step Function when loan applications files are uploaded to raw bucket"

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = {
        name = [var.raw_bucket_name]
      }
      object = {
        key = [
          {
            prefix = "loan_applications/raw/"
          }
        ]
      }
    }
  })

  tags = merge(
    var.tags,
    {
      Name = "${var.state_machine_name}-loan-apps-trigger"
    }
  )
}

resource "aws_cloudwatch_event_target" "loan_applications_sfn_target" {
  rule      = aws_cloudwatch_event_rule.loan_applications_trigger.name
  target_id = "StepFunctionTarget"
  arn       = aws_sfn_state_machine.credit_risk_workflow.arn
  role_arn  = aws_iam_role.eventbridge_sfn_role.arn
}

# EventBridge Rule: Credit Bureau
resource "aws_cloudwatch_event_rule" "credit_bureau_trigger" {
  name        = "${var.state_machine_name}-credit-bureau-trigger"
  description = "Trigger Step Function when credit bureau files are uploaded to raw bucket"

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = {
        name = [var.raw_bucket_name]
      }
      object = {
        key = [
          {
            prefix = "credit_bureau/raw/"
          }
        ]
      }
    }
  })

  tags = merge(
    var.tags,
    {
      Name = "${var.state_machine_name}-credit-bureau-trigger"
    }
  )
}

resource "aws_cloudwatch_event_target" "credit_bureau_sfn_target" {
  rule      = aws_cloudwatch_event_rule.credit_bureau_trigger.name
  target_id = "StepFunctionTarget"
  arn       = aws_sfn_state_machine.credit_risk_workflow.arn
  role_arn  = aws_iam_role.eventbridge_sfn_role.arn
}

# EventBridge Rule: Loan Repayments
resource "aws_cloudwatch_event_rule" "loan_repayments_trigger" {
  name        = "${var.state_machine_name}-loan-repayments-trigger"
  description = "Trigger Step Function when loan repayments files are uploaded to raw bucket"

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = {
        name = [var.raw_bucket_name]
      }
      object = {
        key = [
          {
            prefix = "loan_repayments/raw/"
          }
        ]
      }
    }
  })

  tags = merge(
    var.tags,
    {
      Name = "${var.state_machine_name}-loan-repayments-trigger"
    }
  )
}

resource "aws_cloudwatch_event_target" "loan_repayments_sfn_target" {
  rule      = aws_cloudwatch_event_rule.loan_repayments_trigger.name
  target_id = "StepFunctionTarget"
  arn       = aws_sfn_state_machine.credit_risk_workflow.arn
  role_arn  = aws_iam_role.eventbridge_sfn_role.arn
}
