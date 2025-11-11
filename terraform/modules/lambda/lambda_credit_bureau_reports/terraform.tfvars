# environment = "dev"

# Landing bucket configuration (where CSV files are dropped)
landing_bucket_name = "de-creditrisk"
landing_prefix      = "credit_bureau/" # Path within bucket

# Processed bucket configuration (where files are routed after validation)
processed_bucket_name = "creditrisk-s3-test-bucket-011"
raw_prefix            = "raw/"      # For validated files
reject_prefix         = "rejected/" # For invalid files

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