variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "credit-risk"
}

variable "region" {
  description = "AWS region"
  type        = string
  default     = "eu-west-1"
}

# Lambda API Poller Variables
variable "api_endpoint" {
  description = "API endpoint URL to poll"
  type        = string
  default     = "https://m7erqpk505.execute-api.eu-west-1.amazonaws.com/dev"
}


variable "s3_bucket_name" {
  description = "S3 bucket name for storing API data"
  type        = string
  default     = "creditrisk-raw01"
}

variable "silver_bucket_name" {
  description = "Name of the silver data S3 bucket for cleansed data"
  type        = string
  default     = "creditrisk-silver"
}

variable "gold_bucket_name" {
  description = "Name of the gold data S3 bucket for dimensional models and KPIs"
  type        = string
  default     = "creditrisk-gold01"
}


#############################################
# CREDIT BUREAU REPORTS INGESTION VARIABLES #
#############################################

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

# variable "environment" {
#   description = "Environment name (dev, staging, prod)"
#   type        = string
# }

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

variable "common_tags" {
  description = "Common tags applied to all resources via provider default_tags"
  type        = map(string)
  default     = {}
}


#############################################
# LOAN APPLICATIONS INGESTION VARIABLES #
#############################################

variable "landing_bucket_name_loan_application" {
  description = "S3 bucket name for landing/source data"
  type        = string
}

variable "landing_prefix_loan_application" {
  description = "Prefix/path within landing bucket (e.g., 'landing/' or 'incoming/daily/')"
  type        = string
}

variable "processed_bucket_name_loan_application" {
  description = "S3 bucket name for processed data (raw and rejected paths)"
  type        = string
}

variable "raw_prefix_loan_application" {
  description = "Prefix/path for validated files in processed bucket"
  type        = string
}

variable "reject_prefix_loan_application" {
  description = "Prefix/path for rejected files in processed bucket"
  type        = string
}


#############################################
# LOAN REPAYMENTS INGESTION VARIABLES #
#############################################

variable "landing_bucket_name_loan_repayments" {
  description = "S3 bucket name for landing/source data"
  type        = string
}

variable "landing_prefix_loan_repayments" {
  description = "Prefix/path within landing bucket (e.g., 'landing/' or 'incoming/daily/')"
  type        = string
}

variable "processed_bucket_name_loan_repayments" {
  description = "S3 bucket name for processed data (raw and rejected paths)"
  type        = string
}

variable "raw_prefix_loan_repayments" {
  description = "Prefix/path for validated files in processed bucket"
  type        = string
}

variable "reject_prefix_loan_repayments" {
  description = "Prefix/path for rejected files in processed bucket"
  type        = string
}

#############################################
# CT TRANSFORMATION VARIABLES #
#############################################

variable "tokenization_salt" {
  description = "Salt value for account number tokenization (sensitive)"
  type        = string
  sensitive   = true
  default     = "CHANGE_IN_PRODUCTION"
}


#############################################
# STEP FUNCTION WORKFLOW VARIABLES #
#############################################

variable "notification_emails" {
  description = "List of email addresses for Step Function failure notifications"
  type        = list(string)
  default     = []
}