# EventBridge Rule (Schedule)
resource "aws_cloudwatch_event_rule" "market_data_schedule" {
  name                = var.schedule_name
  description         = var.schedule_description
  schedule_expression = var.schedule_expression

  tags = {
    Name        = var.schedule_name
    Environment = var.environment
    Project     = var.project_name
  }
}

# EventBridge Target (Lambda)
resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.market_data_schedule.name
  target_id = "MarketDataPollerLambda"
  arn       = var.lambda_function_arn
}

# Lambda Permission for EventBridge to invoke the function
resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = var.lambda_function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.market_data_schedule.arn
}