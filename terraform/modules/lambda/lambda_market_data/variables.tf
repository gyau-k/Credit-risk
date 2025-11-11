variable "function_name" {
  description = "Name of the Lambda function"
  type        = string
}

variable "s3_bucket_name" {
  description = "Name of the S3 bucket for data ingestion"
  type        = string
}

variable "s3_bucket_arn" {
  description = "ARN of the S3 bucket for data ingestion"
  type        = string
}

variable "market_data_api_endpoint" {
  description = "Market data API endpoint URL"
  type        = string
}

variable "log_level" {
  description = "Lambda function log level"
  type        = string
  default     = "INFO"
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 7
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "project_name" {
  description = "Project name for tagging"
  type        = string
}

variable "kms_key_id" {
  description = "KMS key ID for encryption"
  type        = string
  default     = ""
}

variable "kms_key_arn" {
  description = "KMS key ARN for encryption"
  type        = string
  default     = ""
}

