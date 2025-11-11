# Dev Environment Variables
environment  = "dev"
project_name = "credit-risk"
region       = "eu-west-1"

# Lambda API Poller Configuration
api_endpoint = "https://m7erqpk505.execute-api.eu-west-1.amazonaws.com/dev"

s3_bucket_name     = "creditrisk-raw01"
silver_bucket_name = "creditrisk-silver"


#############################################
# CREDIT BUREAU REPORTS INGESTION VARIABLES #
#############################################

# AWS Configuration
aws_region = "us-east-1" # Change to your preferred region

# Landing bucket configuration (where CSV files are dropped)
landing_bucket_name = "de-creditrisk"
landing_prefix      = "credit_bureau/" # Path within bucket

# Processed bucket configuration (where files are routed after validation)
processed_bucket_name = "creditrisk-raw01"
raw_prefix            = "credit_bureau/raw/"      # For validated files
reject_prefix         = "credit_bureau/rejected/" # For invalid files

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

# Common tags applied via provider
common_tags = {
  ManagedBy = "Terraform"
  Owner     = "DataEngineering"
}


#############################################
# LOAN APPLICATIONS INGESTION VARIABLES #
#############################################

# Landing bucket configuration (where CSV files are dropped)
landing_bucket_name_loan_application = "de-creditrisk"
landing_prefix_loan_application      = "loan_applications/" # Path within bucket

# Processed bucket configuration (where files are routed after validation)
processed_bucket_name_loan_application = "creditrisk-raw01"
raw_prefix_loan_application            = "loan_applications/raw/"      # For validated files
reject_prefix_loan_application         = "loan_applications/rejected/" # For invalid files


#############################################
# LOAN REPAYMENTS INGESTION VARIABLES #
#############################################

# Landing bucket configuration (where CSV files are dropped)
landing_bucket_name_loan_repayments = "de-creditrisk"
landing_prefix_loan_repayments      = "loan_repayments/" # Path within bucket

# Processed bucket configuration (where files are routed after validation)
processed_bucket_name_loan_repayments = "creditrisk-raw01"
raw_prefix_loan_repayments            = "loan_repayments/raw/"      # For validated files
reject_prefix_loan_repayments         = "loan_repayments/rejected/" # For invalid files
#############################################
# STEP FUNCTION WORKFLOW VARIABLES #
#############################################

# Email for Step Function failure notifications 
notification_emails = [] 