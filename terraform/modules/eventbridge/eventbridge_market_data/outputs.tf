output "rule_name" {
  description = "Name of the EventBridge rule"
  value       = aws_cloudwatch_event_rule.market_data_schedule.name
}

output "rule_arn" {
  description = "ARN of the EventBridge rule"
  value       = aws_cloudwatch_event_rule.market_data_schedule.arn
}

output "schedule_expression" {
  description = "Schedule expression of the rule"
  value       = aws_cloudwatch_event_rule.market_data_schedule.schedule_expression
}