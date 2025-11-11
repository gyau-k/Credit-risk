variable "function_name" {
  description = "Name of the Lambda function"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "project_name" {
  description = "Project name for tagging"
  type        = string
}

variable "raw_bucket_name" {
  description = "Name of the raw S3 bucket"
  type        = string
}

variable "raw_bucket_arn" {
  description = "ARN of the raw S3 bucket"
  type        = string
}

variable "silver_bucket_name" {
  description = "Name of the silver S3 bucket"
  type        = string
}

variable "silver_bucket_arn" {
  description = "ARN of the silver S3 bucket"
  type        = string
}

variable "raw_prefix" {
  description = "Raw prefix"
  type        = string
  default     = "market_data/raw/"
}

variable "cleansed_prefix" {
  description = "Cleansed prefix"
  type        = string
  default     = "market_data/cleansed/"
}

variable "rejected_prefix" {
  description = "Rejected prefix"
  type        = string
  default     = "market_data/rejected/"
}

variable "processed_prefix" {
  description = "Processed prefix"
  type        = string
  default     = "market_data/processed/"
}

variable "dim_market_prefix" {
  description = "DimMarket prefix"
  type        = string
  default     = "dim_market/"
}

variable "batch_size" {
  description = "Batch size for processing"
  type        = number
  default     = 1000
}

variable "log_level" {
  description = "Log level"
  type        = string
  default     = "INFO"
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 7
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

