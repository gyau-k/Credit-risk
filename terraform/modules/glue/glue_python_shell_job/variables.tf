variable "job_name" {
  description = "Name of the Glue job"
  type        = string
}

variable "job_description" {
  description = "Description of the Glue job"
  type        = string
  default     = "FnD and kpis"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

variable "script_path" {
  description = "Local path to the Python script file"
  type        = string
}

variable "requirements_path" {
  description = "Local path to the requirements.txt file (optional)"
  type        = string
  default     = null
}

variable "scripts_bucket" {
  description = "S3 bucket name for storing Glue scripts"
  type        = string
}

variable "scripts_prefix" {
  description = "S3 prefix/path for Glue scripts"
  type        = string
  default     = "glue/scripts/"
}

variable "s3_buckets" {
  description = "List of S3 bucket names the Glue job needs access to"
  type        = list(string)
}

variable "python_version" {
  description = "Python version for Python Shell job (3, 3.9, etc.)"
  type        = string
  default     = "3.9"
}

variable "glue_version" {
  description = "Glue version"
  type        = string
  default     = "3.0"
}

variable "max_capacity" {
  description = "Maximum DPU (Data Processing Units) for Python Shell job"
  type        = number
  default     = 0.0625
}

variable "max_retries" {
  description = "Maximum number of retries on job failure"
  type        = number
  default     = 0
}

variable "timeout_minutes" {
  description = "Job timeout in minutes"
  type        = number
  default     = 60
}

variable "additional_python_modules" {
  description = "List of additional Python modules to install (e.g., ['pandas==2.0.3'])"
  type        = list(string)
  default     = null
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 7
}

variable "data_source" {
  description = "Data source identifier for tagging"
  type        = string
  default     = "credit-risk"
}

variable "table_name" {
  description = "Table name for tagging"
  type        = string
  default     = "cred-risk-glue"
}
