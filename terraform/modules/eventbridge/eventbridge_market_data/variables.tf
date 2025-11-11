variable "schedule_name" {
  description = "Name of the EventBridge schedule"
  type        = string
}

variable "schedule_description" {
  description = "Description of the EventBridge schedule"
  type        = string
}

variable "schedule_expression" {
  description = "Schedule expression (rate or cron)"
  type        = string

  validation {
    condition     = can(regex("^(rate|cron)\\(.*\\)$", var.schedule_expression))
    error_message = "Schedule expression must be a valid rate() or cron() expression."
  }
}

variable "lambda_function_arn" {
  description = "ARN of the Lambda function to invoke"
  type        = string
}

variable "lambda_function_name" {
  description = "Name of the Lambda function to invoke"
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