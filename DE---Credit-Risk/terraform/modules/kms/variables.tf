# KMS Module Variables

variable "project_name" {
  description = "credit-risk"
  type        = string
}

variable "environment" {
  description = "dev"
  type        = string
}

variable "aws_account_id" {
  description = "AWS Account ID for KMS key policy"
  type        = string
}

variable "aws_region" {
  description = "AWS Region"
  type        = string
  default     = "us-east-1"
}

variable "lambda_role_arns" {
  description = "List of Lambda execution role ARNs that need access to this KMS key"
  type        = list(string)
  default     = []
}

variable "deletion_window_in_days" {
  description = "Duration in days after which the key is deleted after destruction of the resource"
  type        = number
  default     = 30
}

variable "enable_key_rotation" {
  description = "Enable automatic key rotation for compliance"
  type        = bool
  default     = true
}

variable "tags" {
  description = "Additional tags to apply to the KMS key"
  type        = map(string)
  default     = {}
}

