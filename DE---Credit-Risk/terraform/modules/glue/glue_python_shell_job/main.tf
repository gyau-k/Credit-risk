/*
 * Glue Python Shell Job Module
 * Creates a reusable Glue Python Shell job with script upload and IAM configuration
 */

locals {
  script_filename = basename(var.script_path)
  requirements_filename = var.requirements_path != null ? basename(var.requirements_path) : null

  common_tags = {
    Environment = var.environment
    ManagedBy   = "Terraform"
    Service     = "glue-job"
    JobName     = var.job_name
  }
}

# Upload Python script to S3
resource "aws_s3_object" "glue_script" {
  bucket = var.scripts_bucket
  key    = "${var.scripts_prefix}${local.script_filename}"
  source = var.script_path
  etag   = filemd5(var.script_path)

  tags = local.common_tags
}

# Upload requirements.txt if provided
resource "aws_s3_object" "glue_requirements" {
  count = var.requirements_path != null ? 1 : 0

  bucket = var.scripts_bucket
  key    = "${var.scripts_prefix}${local.requirements_filename}"
  source = var.requirements_path
  etag   = filemd5(var.requirements_path)

  tags = local.common_tags
}

# IAM Role for Glue Job
resource "aws_iam_role" "glue_job_role" {
  name = "${var.job_name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })

  tags = local.common_tags
}

# Attach AWS managed Glue service policy
resource "aws_iam_role_policy_attachment" "glue_service_policy" {
  role       = aws_iam_role.glue_job_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# IAM Policy for S3 bucket access
resource "aws_iam_policy" "glue_s3_policy" {
  name        = "${var.job_name}-s3-policy"
  description = "S3 access for Glue job ${var.job_name}"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadWriteDataBuckets"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = concat(
          [for bucket in var.s3_buckets : "arn:aws:s3:::${bucket}"],
          [for bucket in var.s3_buckets : "arn:aws:s3:::${bucket}/*"]
        )
      },
      {
        Sid    = "AccessScriptsBucket"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion"
        ]
        Resource = [
          "arn:aws:s3:::${var.scripts_bucket}/${var.scripts_prefix}*"
        ]
      }
    ]
  })

  tags = local.common_tags
}

# Attach S3 policy to Glue role
resource "aws_iam_role_policy_attachment" "glue_s3_attach" {
  role       = aws_iam_role.glue_job_role.name
  policy_arn = aws_iam_policy.glue_s3_policy.arn
}

# CloudWatch Logs Policy
resource "aws_iam_policy" "glue_cloudwatch_policy" {
  name        = "${var.job_name}-cloudwatch-policy"
  description = "CloudWatch Logs access for Glue job ${var.job_name}"

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
        Resource = [
          "arn:aws:logs:*:*:log-group:/aws-glue/python-jobs/*"
        ]
      }
    ]
  })

  tags = local.common_tags
}

# Attach CloudWatch policy
resource "aws_iam_role_policy_attachment" "glue_cloudwatch_attach" {
  role       = aws_iam_role.glue_job_role.name
  policy_arn = aws_iam_policy.glue_cloudwatch_policy.arn
}

# Glue Python Shell Job
resource "aws_glue_job" "python_shell_job" {
  name     = var.job_name
  role_arn = aws_iam_role.glue_job_role.arn

  command {
    name            = "pythonshell"
    script_location = "s3://${var.scripts_bucket}/${var.scripts_prefix}${local.script_filename}"
    python_version  = var.python_version
  }

  glue_version = var.glue_version
  max_capacity = var.max_capacity

  max_retries = var.max_retries
  timeout     = var.timeout_minutes

  # Default arguments for Python modules
  default_arguments = merge(
    {
      "--job-language"                     = "python"
      "--job-bookmark-option"              = "job-bookmark-disable"
      "--enable-metrics"                   = "true"
      "--enable-continuous-cloudwatch-log" = "true"
      "--TempDir"                          = "s3://${var.scripts_bucket}/glue/temp/"
    },
    var.additional_python_modules != null && length(var.additional_python_modules) > 0 ? {
      "--additional-python-modules" = join(",", var.additional_python_modules)
    } : {}
  )

  description = var.job_description

  tags = merge(
    local.common_tags,
    {
      DataSource = var.data_source
      TableName  = var.table_name
    }
  )

  depends_on = [
    aws_s3_object.glue_script,
    aws_iam_role_policy_attachment.glue_service_policy,
    aws_iam_role_policy_attachment.glue_s3_attach,
    aws_iam_role_policy_attachment.glue_cloudwatch_attach
  ]
}
