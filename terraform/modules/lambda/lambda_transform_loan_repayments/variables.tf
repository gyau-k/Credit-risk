variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

variable "raw_bucket_name" {
  description = "S3 bucket name for raw data"
  type        = string
}

variable "silver_bucket_name" {
  description = "S3 bucket name for silver/transformed data"
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
  default     = 300
}

variable "memory_size" {
  description = "Lambda function memory in MB"
  type        = number
  default     = 1024
}

variable "log_level" {
  description = "Lambda logging level"
  type        = string
  default     = "INFO"
}

variable "log_retention_days" {
  description = "CloudWatch log retention period in days"
  type        = number
  default     = 7
}

variable "tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default     = {}
}
