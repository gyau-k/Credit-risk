####################################################
# Outputs for Step Functions Module
####################################################

output "state_machine_arn" {
  description = "ARN of the Step Functions state machine"
  value       = aws_sfn_state_machine.credit_risk_workflow.arn
}

output "state_machine_name" {
  description = "Name of the Step Functions state machine"
  value       = aws_sfn_state_machine.credit_risk_workflow.name
}

output "state_machine_id" {
  description = "ID of the Step Functions state machine"
  value       = aws_sfn_state_machine.credit_risk_workflow.id
}

output "sfn_role_arn" {
  description = "ARN of the Step Functions IAM role"
  value       = aws_iam_role.sfn_role.arn
}

output "sfn_role_name" {
  description = "Name of the Step Functions IAM role"
  value       = aws_iam_role.sfn_role.name
}

output "eventbridge_role_arn" {
  description = "ARN of the EventBridge IAM role"
  value       = aws_iam_role.eventbridge_sfn_role.arn
}

output "sns_topic_arn" {
  description = "ARN of the SNS topic for failure notifications"
  value       = aws_sns_topic.workflow_failures.arn
}

output "log_group_name" {
  description = "Name of the CloudWatch log group"
  value       = aws_cloudwatch_log_group.sfn_logs.name
}

output "eventbridge_rule_arns" {
  description = "ARNs of the EventBridge rules"
  value = {
    loan_applications = aws_cloudwatch_event_rule.loan_applications_trigger.arn
    credit_bureau     = aws_cloudwatch_event_rule.credit_bureau_trigger.arn
    loan_repayments   = aws_cloudwatch_event_rule.loan_repayments_trigger.arn
  }
}
