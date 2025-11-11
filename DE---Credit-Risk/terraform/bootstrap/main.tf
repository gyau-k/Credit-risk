# S3 backend
resource "aws_s3_bucket" "mybucket" {
  bucket = "s3statebackend-credit-risk-011"

  lifecycle {
    prevent_destroy = true
  }
}

# Enable versioning on the bucket
resource "aws_s3_bucket_versioning" "mybucket" {
  bucket = aws_s3_bucket.mybucket.id

  versioning_configuration {
    status = "Enabled"
  }

  lifecycle {
    prevent_destroy = true
  }
}

# Create Dynamodb
resource "aws_dynamodb_table" "statelock" {
  name         = "state-lock"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket" "creditrisk-raw01" {
  bucket        = "creditrisk-raw01"
  force_destroy = false

  lifecycle {
    prevent_destroy = true
  }

  tags = {
    Environment = "dev"
    Purpose     = "raw-data-storage"
  }
}
resource "aws_s3_bucket" "creditrisk-silver" {
  bucket        = "creditrisk-silver"
  force_destroy = false

  lifecycle {
    prevent_destroy = true
  }

  tags = {
    Environment = "dev"
    Purpose     = "silver-data-storage"
  }
}

resource "aws_s3_bucket" "creditrisk-gold01" {
  bucket        = "creditrisk-gold01"
  force_destroy = false

  lifecycle {
    prevent_destroy = true
  }

  tags = {
    Environment = "dev"
    Purpose     = "gold-data-storage"
  }
}