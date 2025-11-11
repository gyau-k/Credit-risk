# environment = "dev"

# Landing bucket configuration (where CSV files are dropped)
landing_bucket_name_loan_application = "de-creditrisk"
landing_prefix_loan_application      = "loan_applications/" # Path within bucket

# Processed bucket configuration (where files are routed after validation)
processed_bucket_name_loan_application = "creditrisk-raw01"
raw_prefix_loan_application            = "loan_applications/raw/"      # For validated files
reject_prefix_loan_application         = "loan_applications/rejected/" # For invalid files

# Lambda configuration
lambda_timeout                 = 60     # seconds
lambda_memory_size             = 256    # MB
reserved_concurrent_executions = 10     # Limit concurrent executions for cost control
log_level                      = "INFO" # DEBUG, INFO, WARNING, ERROR

# CloudWatch configuration
log_retention_days = 30 # Days to retain logs

# Tags for all resources
tags = {
  Project    = "DataIngestion"
  Team       = "DataEngineering"
  CostCenter = "Analytics"
  Compliance = "PII-Sensitive"
}