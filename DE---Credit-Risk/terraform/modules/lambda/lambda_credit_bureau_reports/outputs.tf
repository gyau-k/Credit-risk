# Outputs for reference
output "lambda_function_name_credit" {
  description = "Name of the Lambda function"
  value       = aws_lambda_function.csv_validator.function_name
}

output "lambda_function_arn_credit" {
  description = "ARN of the Lambda function"
  value       = aws_lambda_function.csv_validator.arn
}

output "lambda_role_arn_credit" {
  description = "ARN of the Lambda execution role"
  value       = aws_iam_role.lambda_role.arn
}

output "cloudwatch_log_group_credit" {
  description = "CloudWatch log group name"
  value       = aws_cloudwatch_log_group.lambda_logs.name
}

output "source_path" {
  description = "Full source path (landing bucket and prefix)"
  value       = local.source_path
}

output "raw_path" {
  description = "Full raw path (processed bucket and prefix)"
  value       = local.raw_path
}

output "reject_path" {
  description = "Full reject path (processed bucket and prefix)"
  value       = local.reject_path
}