# Outputs for reference
output "lambda_function_name_loan_repayments" {
  description = "Name of the Lambda function"
  value       = aws_lambda_function.csv_validator_loan_repayments.function_name
}

output "lambda_function_arn_loan_repayments" {
  description = "ARN of the Lambda function"
  value       = aws_lambda_function.csv_validator_loan_repayments.arn
}

output "lambda_role_arn_loan_repayments" {
  description = "ARN of the Lambda execution role"
  value       = aws_iam_role.lambda_role.arn
}

output "cloudwatch_log_group_loan_repayments" {
  description = "CloudWatch log group name"
  value       = aws_cloudwatch_log_group.lambda_logs.name
}

output "source_path_loan_repayments" {
  description = "Full source path (landing bucket and prefix)"
  value       = local.source_path
}

output "raw_path_loan_repayments" {
  description = "Full raw path (processed bucket and prefix)"
  value       = local.raw_path
}

output "reject_path_loan_repayments" {
  description = "Full reject path (processed bucket and prefix)"
  value       = local.reject_path
}