# Variables for easy configuration
variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "landing_bucket_name" {
  description = "S3 bucket name for landing/source data"
  type        = string
}

variable "landing_prefix" {
  description = "Prefix/path within landing bucket (e.g., 'landing/' or 'incoming/daily/')"
  type        = string
}

variable "processed_bucket_name" {
  description = "S3 bucket name for processed data (raw and rejected paths)"
  type        = string
}

variable "raw_prefix" {
  description = "Prefix/path for validated files in processed bucket"
  type        = string
}

variable "reject_prefix" {
  description = "Prefix/path for rejected files in processed bucket"
  type        = string
}

variable "lambda_timeout" {
  description = "Lambda function timeout in seconds"
  type        = number
  default     = 60
}

variable "lambda_memory_size" {
  description = "Lambda function memory in MB"
  type        = number
  default     = 256
}

variable "log_level" {
  description = "Lambda logging level (DEBUG, INFO, WARNING, ERROR)"
  type        = string
  default     = "INFO"
}

variable "log_retention_days" {
  description = "CloudWatch log retention period in days"
  type        = number
  default     = 30
}

variable "reserved_concurrent_executions" {
  description = "Number of reserved concurrent executions (use -1 for unreserved)"
  type        = number
  default     = 10
}

variable "tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default     = {}
}

variable "kms_key_id" {
  description = "KMS key ID for S3 encryption"
  type        = string
}

variable "kms_key_arn" {
  description = "KMS key ARN for IAM permissions"
  type        = string
}
