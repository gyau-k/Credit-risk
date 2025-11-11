# Dev Environment Configuration
# Local variables for dev environment
locals {
  environment  = "dev"
  project_name = "credit-risk"

  # Environment-specific naming
  name_prefix = "${local.project_name}-${local.environment}"
}

# Data source to get AWS account ID
data "aws_caller_identity" "current" {}

# ---------------------------------------
# KMS Key for Data Encryption
# Shared by market data and customer transaction pipelines
# ---------------------------------------
module "kms_data_encryption" {
  source = "../../modules/kms"

  project_name   = local.project_name
  environment    = local.environment
  aws_account_id = data.aws_caller_identity.current.account_id
  aws_region     = "eu-west-1"

  # Grant access to all Lambda functions that need encryption
  lambda_role_arns = [
    module.lambda_market_data_poller.role_arn,
    module.market_data_transformer.lambda_role_arn,
    module.lambda_api_poller.role_arn,
    module.lambda_ct_transformation.lambda_role_arn
  ]

  deletion_window_in_days = 30
  enable_key_rotation     = true
}

# Lambda API Poller Module
module "lambda_api_poller" {
  source = "../../modules/lambda/lambda_customer_transaction"

  function_name = "${local.name_prefix}-api-poller"
  environment   = local.environment
  project_name  = local.project_name

  # API Configuration
  api_endpoint = var.api_endpoint

  # S3 Configuration
  s3_bucket_name  = var.s3_bucket_name
  s3_valid_prefix = "customer-transactions/raw/valid/"
  s3_error_prefix = "customer-transactions/raw/error/"

  # Security Configuration - KMS Encryption
  kms_key_id  = module.kms_data_encryption.kms_key_id
  kms_key_arn = module.kms_data_encryption.kms_key_arn

  # Lambda Configuration
  runtime                   = "python3.11"
  timeout                   = 900 # 15 minutes
  memory_size               = 1024
  request_timeout           = 30
  max_transactions_per_poll = 5000

  # Scheduling - every 1 minute
  schedule_expression = "rate(1 minute)"

  # Logging
  log_retention_days = 7
}

# ---------------------------------------
# Lambda function for market data polling
# ---------------------------------------
module "lambda_market_data_poller" {
  source = "../../modules/lambda/lambda_market_data"

  function_name = "${local.name_prefix}-market-data-poller"
  environment   = local.environment
  project_name  = local.project_name

  market_data_api_endpoint = "https://gwu87ggeg4.execute-api.eu-west-1.amazonaws.com/dev"
  s3_bucket_name           = var.s3_bucket_name
  s3_bucket_arn            = "arn:aws:s3:::${var.s3_bucket_name}"
  kms_key_id               = module.kms_data_encryption.kms_key_id
  kms_key_arn              = module.kms_data_encryption.kms_key_arn
  log_level                = "INFO"
  log_retention_days       = 7
}

# ----------------------------------------------------------
# EventBridge schedule to trigger Lambda to poll market data
# ----------------------------------------------------------
module "eventbridge_market_data_schedule" {
  source = "../../modules/eventbridge/eventbridge_market_data"

  schedule_name        = "${local.name_prefix}-market-data-schedule"
  schedule_description = "Trigger market data polling"
  schedule_expression  = "rate(60 minutes)"
  lambda_function_arn  = module.lambda_market_data_poller.function_arn
  lambda_function_name = module.lambda_market_data_poller.function_name
  environment          = local.environment
  project_name         = local.project_name
}


#################################################
# Credit bureau reports ingestion Lambda module #
#################################################
module "credit_bureau_validator" {
  source = "../../modules/lambda/lambda_credit_bureau_reports"

  # Environment
  environment = var.environment

  # S3 bucket configuration
  landing_bucket_name   = var.landing_bucket_name
  landing_prefix        = var.landing_prefix
  processed_bucket_name = var.processed_bucket_name
  raw_prefix            = var.raw_prefix
  reject_prefix         = var.reject_prefix

  # Security Configuration - KMS Encryption
  kms_key_id  = module.kms_data_encryption.kms_key_id
  kms_key_arn = module.kms_data_encryption.kms_key_arn

  # Lambda configuration
  lambda_timeout                 = var.lambda_timeout
  lambda_memory_size             = var.lambda_memory_size
  reserved_concurrent_executions = var.reserved_concurrent_executions
  log_level                      = var.log_level

  # CloudWatch configuration
  log_retention_days = var.log_retention_days

  # Tags
  tags = var.tags
}


#################################################
# Loan Applications ingestion Lambda module #
#################################################
module "loan_applications_validator" {
  source = "../../modules/lambda/lambda_loan_applications"

  # # Environment
  # environment = var.environment

  # S3 bucket configuration
  raw_prefix_loan_application            = var.raw_prefix_loan_application
  reject_prefix_loan_application         = var.reject_prefix_loan_application
  processed_bucket_name_loan_application = var.processed_bucket_name_loan_application
  landing_prefix_loan_application        = var.landing_prefix_loan_application
  landing_bucket_name_loan_application   = var.landing_bucket_name_loan_application

  # Security Configuration - KMS Encryption
  kms_key_id  = module.kms_data_encryption.kms_key_id
  kms_key_arn = module.kms_data_encryption.kms_key_arn

}


#################################################
# Loan Repayments ingestion Lambda module #
#################################################
module "loan_repayments_validator" {
  source = "../../modules/lambda/lambda_loan_repayments"

  # # Environment
  # environment = var.environment

  # S3 bucket configuration
  raw_prefix_loan_repayments            = var.raw_prefix_loan_repayments
  reject_prefix_loan_repayments         = var.reject_prefix_loan_repayments
  processed_bucket_name_loan_repayments = var.processed_bucket_name_loan_repayments
  landing_prefix_loan_repayments        = var.landing_prefix_loan_repayments
  landing_bucket_name_loan_repayments   = var.landing_bucket_name_loan_repayments

  # Security Configuration - KMS Encryption
  kms_key_id  = module.kms_data_encryption.kms_key_id
  kms_key_arn = module.kms_data_encryption.kms_key_arn

}

# -----------------------------------------
#  Market Data Transformer Lambda Module
# -----------------------------------------
module "market_data_transformer" {
  source = "../../modules/lambda/lambda_market_data_transformer"

  function_name = "${local.name_prefix}-market-data-transformer"
  environment   = local.environment
  project_name  = local.project_name

  # S3 Configuration
  raw_bucket_name    = var.s3_bucket_name
  raw_bucket_arn     = "arn:aws:s3:::${var.s3_bucket_name}"
  silver_bucket_name = var.silver_bucket_name
  silver_bucket_arn  = "arn:aws:s3:::${var.silver_bucket_name}"

  # Security Configuration
  kms_key_id  = module.kms_data_encryption.kms_key_id
  kms_key_arn = module.kms_data_encryption.kms_key_arn

  # Processing Configuration
  batch_size         = 1000
  log_level          = "INFO"
  log_retention_days = 7
}



#################################################
# Customer Transaction Transformation Lambda #
#################################################
module "lambda_ct_transformation" {
  source = "../../modules/lambda/lambda_ct_transformation"

  # Function configuration
  function_name = "${local.name_prefix}-ct-transformation"
  environment   = local.environment
  project_name  = local.project_name

  # S3 Configuration
  s3_raw_bucket    = var.s3_bucket_name # creditrisk-raw01
  s3_silver_bucket = "creditrisk-silver"

  # S3 Trigger Configuration
  s3_trigger_prefix = "customer-transactions/raw/valid/"
  s3_trigger_suffix = ".json"

  # S3 Output Paths (no trailing slashes)
  s3_transformed_prefix = "customer-transactions/transformed"
  s3_fact_prefix        = "customer-transactions/FactTransactions"
  s3_error_prefix       = "customer-transactions/error"

  # Security - PII Masking
  tokenization_salt = var.tokenization_salt

  # Security Configuration - KMS Encryption
  kms_key_id  = module.kms_data_encryption.kms_key_id
  kms_key_arn = module.kms_data_encryption.kms_key_arn

  # Lambda Configuration
  runtime     = "python3.11"
  timeout     = 900  # 15 minutes
  memory_size = 2048 # Higher memory

  # Delta Lake Configuration
  delta_write_mode = "append"

  # Logging
  log_retention_days = 7
}


#################################################
# TRANSFORMATION JOBS (Bronze → Silver) - Now using Lambda
#################################################

# Lambda: Transform Loan Applications
module "lambda_transform_loan_applications" {
  source = "../../modules/lambda/lambda_transform_loan_applications"

  environment        = local.environment
  raw_bucket_name    = var.s3_bucket_name
  silver_bucket_name = var.silver_bucket_name
  runtime            = "python3.11"
  timeout            = 300
  memory_size        = 1024
  log_level          = "INFO"
  log_retention_days = 7

  tags = {
    Project     = "credit-risk"
    Environment = local.environment
    Dataset     = "loan-applications"
  }
}

# Lambda: Transform Loan Repayments
module "lambda_transform_loan_repayments" {
  source = "../../modules/lambda/lambda_transform_loan_repayments"

  environment        = local.environment
  raw_bucket_name    = var.s3_bucket_name
  silver_bucket_name = var.silver_bucket_name
  runtime            = "python3.11"
  timeout            = 300
  memory_size        = 1024
  log_level          = "INFO"
  log_retention_days = 7

  tags = {
    Project     = "credit-risk"
    Environment = local.environment
    Dataset     = "loan-repayments"
  }
}

# Lambda: Transform Credit Bureau
module "lambda_transform_credit_bureau" {
  source = "../../modules/lambda/lambda_transform_credit_bureau"

  environment        = local.environment
  raw_bucket_name    = var.s3_bucket_name
  silver_bucket_name = var.silver_bucket_name
  runtime            = "python3.11"
  timeout            = 300
  memory_size        = 1024
  log_level          = "INFO"
  log_retention_days = 7

  tags = {
    Project     = "credit-risk"
    Environment = local.environment
    Dataset     = "credit-bureau"
  }
}

# -----------------------------------------------------------------------------------
# Enable EventBridge for Step Function (Loan Apps, Loan Repayments, Credit Bureau)
# ------------------------------------------------------------------------------------

resource "aws_s3_bucket_notification" "raw_bucket_eventbridge" {
  bucket = var.s3_bucket_name
  # Enable EventBridge for Step Function (Loan Apps, Loan Repayments, Credit Bureau)
  eventbridge = true
  # Direct trigger for Customer Transactions (poller writes, transformer processes)
  lambda_function {
    id                  = "customer-transactions-trigger"
    lambda_function_arn = module.lambda_ct_transformation.lambda_function_arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "customer-transactions/raw/valid/"
    filter_suffix       = ".json"
  }
  # Direct trigger for Market Data (poller writes, transformer processes)
  lambda_function {
    id                  = "market-data-trigger"
    lambda_function_arn = module.market_data_transformer.lambda_function_arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "market_data/raw/"
    filter_suffix       = ".json"
  }
  depends_on = [
    module.lambda_ct_transformation,
    module.market_data_transformer
  ]
}


################################################
# STAGE 2: GOLD ANALYTICS JOB (Silver → Gold)
################################################

# Glue Job: Build Gold Analytics (Dimensions + Facts + KPIs)
module "glue_gold_analytics" {
  source = "../../modules/glue/glue_python_shell_job"

  job_name        = "${local.name_prefix}-gold-analytics"
  job_description = "Build dimensions, facts, and KPIs from silver transformed data"
  environment     = local.environment

  # Script configuration
  script_path       = "../../../src/glue/build_gold_layer/build_gold_analytics.py"
  requirements_path = "../../../src/glue/build_gold_layer/requirements.txt"

  # S3 configuration
  scripts_bucket = var.s3_bucket_name
  scripts_prefix = "glue/scripts/"

  # S3 bucket access - Now writes dimensions/facts to silver, KPIs to gold
  s3_buckets = [
    var.silver_bucket_name, # creditrisk-silver (input + output for dimensions/facts)
    var.gold_bucket_name    # creditrisk-gold01 (output for KPIs only)
  ]

  # Python configuration
  python_version = "3.9"
  glue_version   = "3.0"
  max_capacity   = 0.0625 #  DPU

  # Job parameters
  max_retries     = 1
  timeout_minutes = 90

  # Python modules
  additional_python_modules = [
    "pandas==2.0.3",
    "pyarrow==12.0.1",
    "s3fs==2023.6.0",
    "fsspec==2023.6.0",
    "numpy==1.24.3"
  ]

  # Logging
  log_retention_days = 7

  # Tagging
  data_source = "silver_transformed"
  table_name  = "gold_analytics"
}


#################################################
# Lambda: Check Glue Status
#################################################

module "lambda_check_glue_status" {
  source = "../../modules/lambda/lambda_check_glue_status"

  function_name      = "${local.name_prefix}-check-glue-status"
  environment        = local.environment
  lambda_source_dir  = "../../../src/lambda/lambda_check_glue_status"
  glue_job_name      = module.glue_gold_analytics.job_name
  aws_region         = var.region
  aws_account_id     = data.aws_caller_identity.current.account_id
  log_retention_days = 7

  tags = {
    Project     = local.project_name
    Environment = local.environment
  }
}


#################################################
# Step Function: Credit Risk Workflow
#################################################
module "credit_risk_workflow" {
  source = "../../modules/stepfunctions/credit_risk_transformation"

  state_machine_name = "${local.name_prefix}-credit-risk-workflow"
  environment        = local.environment
  aws_region         = var.region
  aws_account_id     = data.aws_caller_identity.current.account_id

  # Lambda ARNs
  lambda_transform_loan_applications_arn = module.lambda_transform_loan_applications.lambda_function_arn
  lambda_transform_credit_bureau_arn     = module.lambda_transform_credit_bureau.lambda_function_arn
  lambda_transform_loan_repayments_arn   = module.lambda_transform_loan_repayments.lambda_function_arn
  lambda_check_glue_status_arn           = module.lambda_check_glue_status.lambda_function_arn

  # Glue job
  glue_job_name = module.glue_gold_analytics.job_name

  # S3 buckets
  raw_bucket_name    = var.s3_bucket_name
  silver_bucket_name = var.silver_bucket_name

  # Notifications
  notification_emails = var.notification_emails

  # Monitoring
  log_retention_days  = 7
  enable_xray_tracing = false

  tags = {
    Project     = local.project_name
    Environment = local.environment
  }
}


