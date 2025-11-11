# Variables for Lambda CT Transformation module

variable "function_name" {
  description = "Name of the Lambda function"
  type        = string
}

variable "environment" {
  description = "Environment name (dev)"
  type        = string
}

variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "credit-risk"
}

variable "runtime" {
  description = "Lambda runtime"
  type        = string
  default     = "python3.11"
}

variable "timeout" {
  description = "Lambda function timeout in seconds"
  type        = number
  default     = 900 # 15 minutes 
}

variable "memory_size" {
  description = "Lambda function memory size in MB"
  type        = number
  default     = 2048 
}

variable "s3_raw_bucket" {
  description = "S3 bucket name containing raw transaction data"
  type        = string
}

variable "s3_silver_bucket" {
  description = "S3 bucket name for silver layer data"
  type        = string
}

variable "s3_transformed_prefix" {
  description = "S3 prefix for storing transformed data"
  type        = string
  default     = "customer-transactions/transformed/"
}

variable "s3_fact_prefix" {
  description = "S3 prefix for storing fact table data"
  type        = string
  default     = "customer-transactions/FactTransactions/"
}

variable "s3_error_prefix" {
  description = "S3 prefix for storing error records"
  type        = string
  default     = "customer-transactions/error/"
}

variable "tokenization_salt" {
  description = "Salt value for account number tokenization (sensitive)"
  type        = string
  sensitive   = true
}

variable "delta_write_mode" {
  description = "Delta Lake write mode (append, overwrite)"
  type        = string
  default     = "append"
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 7
}

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "eu-west-1"
}

variable "s3_trigger_prefix" {
  description = "S3 prefix to trigger Lambda on file creation"
  type        = string
  default     = "customer-transactions/raw/valid/"
}

variable "s3_trigger_suffix" {
  description = "S3 file suffix to trigger Lambda on (e.g., .json, .csv)"
  type        = string
  default     = ".json"
}

variable "kms_key_id" {
  description = "KMS key ID for S3 encryption"
  type        = string
}

variable "kms_key_arn" {
  description = "KMS key ARN for IAM permissions"
  type        = string
}
