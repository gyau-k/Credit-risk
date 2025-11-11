# Variables for Lambda API Poller module

variable "function_name" {
  description = "Name of the Lambda function"
  type        = string
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

variable "project_name" {
  description = "Project name"
  type        = string
}

variable "runtime" {
  description = "Lambda runtime"
  type        = string
  default     = "python3.11"
}

variable "timeout" {
  description = "Lambda function timeout in seconds"
  type        = number
  default     = 60
}

variable "memory_size" {
  description = "Lambda function memory size in MB"
  type        = number
  default     = 256
}

variable "api_endpoint" {
  description = "API endpoint URL to poll"
  type        = string
}

variable "s3_bucket_name" {
  description = "S3 bucket name to store polled data"
  type        = string
}

variable "s3_valid_prefix" {
  description = "S3 prefix (folder) for storing valid transactions"
  type        = string
  default     = "customer-transactions/raw/valid/"
}

variable "s3_error_prefix" {
  description = "S3 prefix (folder) for storing invalid transactions"
  type        = string
  default     = "customer-transactions/raw/error/"
}

variable "s3_prefix" {
  description = "(Deprecated) S3 prefix (folder) for storing data - use s3_valid_prefix instead"
  type        = string
  default     = "api-data"
}

variable "request_timeout" {
  description = "API request timeout in seconds"
  type        = number
  default     = 30
}

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "eu-west-1"
}

variable "max_transactions_per_poll" {
  description = "Batch size for processing transactions. All transactions will be processed, but in batches of this size for memory efficiency"
  type        = number
  default     = 1000
}

variable "schedule_expression" {
  description = "EventBridge schedule expression (rate or cron)"
  type        = string
  default     = "rate(1 minute)"
  # Examples:
  # rate(1 minute)
  # rate(5 minutes)
  # rate(1 hour)
  # cron(0 12 * * ? *)  # Every day at noon UTC
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 7
}

variable "kms_key_id" {
  description = "KMS key ID for S3 encryption"
  type        = string
}

variable "kms_key_arn" {
  description = "KMS key ARN for IAM permissions"
  type        = string
}
