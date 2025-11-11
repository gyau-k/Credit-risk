####################################################
# Variables for Step Functions Module
####################################################

variable "state_machine_name" {
  description = "Name of the Step Functions state machine"
  type        = string
}

variable "environment" {
  description = "Environment name (e.g., dev, prod)"
  type        = string
}

variable "aws_region" {
  description = "AWS region"
  type        = string
}

variable "aws_account_id" {
  description = "AWS account ID"
  type        = string
}

# Lambda Function ARNs
variable "lambda_transform_loan_applications_arn" {
  description = "ARN of the Lambda function for loan applications transformation"
  type        = string
}

variable "lambda_transform_credit_bureau_arn" {
  description = "ARN of the Lambda function for credit bureau transformation"
  type        = string
}

variable "lambda_transform_loan_repayments_arn" {
  description = "ARN of the Lambda function for loan repayments transformation"
  type        = string
}

variable "lambda_check_glue_status_arn" {
  description = "ARN of the Lambda function to check Glue job status"
  type        = string
}

# Glue Job Configuration
variable "glue_job_name" {
  description = "Name of the Glue job for building Gold layer"
  type        = string
}

# S3 Bucket Names
variable "raw_bucket_name" {
  description = "Name of the raw (Bronze) S3 bucket"
  type        = string
}

variable "silver_bucket_name" {
  description = "Name of the Silver S3 bucket for error logging"
  type        = string
}

# Notifications
variable "notification_emails" {
  description = "List of email addresses for failure notifications"
  type        = list(string)
  default     = []
}

# Logging and Monitoring
variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 7
}

variable "enable_xray_tracing" {
  description = "Enable AWS X-Ray tracing for the state machine"
  type        = bool
  default     = false
}

# Tags
variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
