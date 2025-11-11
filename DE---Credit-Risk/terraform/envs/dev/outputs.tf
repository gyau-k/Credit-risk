# Environment Information
output "environment" {
  description = "Environment name"
  value       = local.environment
}

output "project_name" {
  description = "Project name"
  value       = local.project_name
}

# ---------------------------------------
# KMS Encryption Key Outputs
# ---------------------------------------
output "kms_data_key_id" {
  description = "KMS key ID for data encryption (shared by market data and customer transactions)"
  value       = module.kms_data_encryption.kms_key_id
}

output "kms_data_key_arn" {
  description = "KMS key ARN for data encryption (shared by market data and customer transactions)"
  value       = module.kms_data_encryption.kms_key_arn
}

output "kms_data_key_alias" {
  description = "KMS key alias for data encryption (shared by market data and customer transactions)"
  value       = module.kms_data_encryption.kms_key_alias
}

# Lambda API Poller Outputs
output "lambda_function_arn" {
  description = "ARN of the Lambda API poller function"
  value       = module.lambda_api_poller.lambda_function_arn
}

output "lambda_function_name" {
  description = "Name of the Lambda API poller function"
  value       = module.lambda_api_poller.lambda_function_name
}

output "lambda_log_group" {
  description = "CloudWatch log group for Lambda function"
  value       = module.lambda_api_poller.cloudwatch_log_group_name
}

output "lambda_schedule" {
  description = "Lambda polling schedule"
  value       = module.lambda_api_poller.schedule_expression
}



#############################################
# CREDIT BUREAU REPORTS INGESTION OUTPUTS   #
#############################################

# Lambda Function outputs
output "lambda_function_name_credit" {
  description = "Name of the CSV validator Lambda function"
  value       = module.credit_bureau_validator.lambda_function_name_credit
}

output "lambda_function_arn_credit" {
  description = "ARN of the CSV validator Lambda function"
  value       = module.credit_bureau_validator.lambda_function_arn_credit
}

output "lambda_role_arn_credit" {
  description = "ARN of the Lambda execution role"
  value       = module.credit_bureau_validator.lambda_role_arn_credit
}

# CloudWatch outputs
output "cloudwatch_log_group_credit" {
  description = "CloudWatch log group name for Lambda"
  value       = module.credit_bureau_validator.cloudwatch_log_group_credit
}

# S3 Path outputs
output "source_path" {
  description = "Full source path (landing bucket and prefix)"
  value       = module.credit_bureau_validator.source_path
}

output "raw_path" {
  description = "Full raw path (processed bucket and prefix)"
  value       = module.credit_bureau_validator.raw_path
}

output "reject_path" {
  description = "Full reject path (processed bucket and prefix)"
  value       = module.credit_bureau_validator.reject_path
}

# Summary output
output "deployment_summary" {
  description = "Summary of deployed resources"
  value = {
    environment      = var.environment
    region           = var.aws_region
    lambda_function  = module.credit_bureau_validator.lambda_function_name_credit
    landing_bucket   = var.landing_bucket_name
    processed_bucket = var.processed_bucket_name
  }
}

#############################################
# LOAN APPLICATIONS INGESTION OUTPUTS   #
#############################################

# Lambda Function outputs
output "lambda_function_name_loan_application" {
  description = "Name of the CSV validator Lambda function"
  value       = module.loan_applications_validator.lambda_function_name_loan_application
}

output "lambda_function_arn_loan_application" {
  description = "ARN of the CSV validator Lambda function"
  value       = module.loan_applications_validator.lambda_function_arn_loan_application
}

output "lambda_role_arn_loan_application" {
  description = "ARN of the Lambda execution role"
  value       = module.loan_applications_validator.lambda_role_arn_loan_application
}

# CloudWatch outputs
output "cloudwatch_log_group_loan_application" {
  description = "CloudWatch log group name for Lambda"
  value       = module.loan_applications_validator.cloudwatch_log_group_loan_application
}

# S3 Path outputs
output "source_path_loan_application" {
  description = "Full source path (landing bucket and prefix)"
  value       = module.loan_applications_validator.source_path_loan_application
}

output "raw_path_loan_application" {
  description = "Full raw path (processed bucket and prefix)"
  value       = module.loan_applications_validator.raw_path_loan_application
}

output "reject_path_loan_application" {
  description = "Full reject path (processed bucket and prefix)"
  value       = module.loan_applications_validator.reject_path_loan_application
}

# Summary output
output "deployment_summary_loan_application" {
  description = "Summary of deployed resources"
  value = {
    environment      = var.environment
    region           = var.aws_region
    lambda_function  = module.loan_applications_validator.lambda_function_name_loan_application
    landing_bucket   = var.landing_bucket_name_loan_application
    processed_bucket = var.processed_bucket_name_loan_application
  }
}


#############################################
# LOAN REPAYMENTS INGESTION OUTPUTS   #
#############################################

# Lambda Function outputs
output "lambda_function_name_loan_repayments" {
  description = "Name of the CSV validator Lambda function"
  value       = module.loan_repayments_validator.lambda_function_name_loan_repayments
}

output "lambda_function_arn_loan_repayments" {
  description = "ARN of the CSV validator Lambda function"
  value       = module.loan_repayments_validator.lambda_function_arn_loan_repayments
}

output "lambda_role_arn_loan_repayments" {
  description = "ARN of the Lambda execution role"
  value       = module.loan_repayments_validator.lambda_role_arn_loan_repayments
}

# CloudWatch outputs
output "cloudwatch_log_group_loan_repayments" {
  description = "CloudWatch log group name for Lambda"
  value       = module.loan_repayments_validator.cloudwatch_log_group_loan_repayments
}

# S3 Path outputs
output "source_path_loan_repayments" {
  description = "Full source path (landing bucket and prefix)"
  value       = module.loan_repayments_validator.source_path_loan_repayments
}

output "raw_path_loan_repayments" {
  description = "Full raw path (processed bucket and prefix)"
  value       = module.loan_repayments_validator.raw_path_loan_repayments
}

output "reject_path_loan_repayments" {
  description = "Full reject path (processed bucket and prefix)"
  value       = module.loan_repayments_validator.reject_path_loan_repayments
}

# Summary output
output "deployment_summary_loan_repayments" {
  description = "Summary of deployed resources"
  value = {
    environment      = var.environment
    region           = var.aws_region
    lambda_function  = module.loan_repayments_validator.lambda_function_name_loan_repayments
    landing_bucket   = var.landing_bucket_name_loan_repayments
    processed_bucket = var.processed_bucket_name_loan_repayments
  }
}

# -------------------------------
# Market Data Transformer Outputs
# -------------------------------
output "market_data_transformer_function_arn" {
  description = "ARN of the market data transformer Lambda function"
  value       = module.market_data_transformer.lambda_function_arn
}

output "market_data_transformer_function_name" {
  description = "Name of the market data transformer Lambda function"
  value       = module.market_data_transformer.lambda_function_name
}