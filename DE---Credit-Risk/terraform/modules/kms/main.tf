# KMS Key for Client-Side Data Encryption
# This key is used by Lambda functions to encrypt data before uploading to S3

resource "aws_kms_key" "data_encryption" {
  description             = "KMS key for encrypting sensitive data in ${var.environment} environment"
  deletion_window_in_days = var.deletion_window_in_days
  enable_key_rotation     = var.enable_key_rotation

  tags = merge(
    var.tags,
    {
      Name        = "${var.project_name}-${var.environment}-data-encryption-key"
      Environment = var.environment
      Purpose     = "Data encryption for S3 objects"
    }
  )
}

# KMS Key Alias for easier reference
resource "aws_kms_alias" "data_encryption_alias" {
  name          = "alias/${var.project_name}-${var.environment}-data-encryption"
  target_key_id = aws_kms_key.data_encryption.key_id
}

# KMS Key Policy
# Allows root account full access and specific permissions for Lambda execution roles
# Note: Lambda permissions are granted via IAM role policies, not the key policy
resource "aws_kms_key_policy" "data_encryption_policy" {
  key_id = aws_kms_key.data_encryption.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat(
      [
        {
          Sid    = "Enable IAM User Permissions"
          Effect = "Allow"
          Principal = {
            AWS = "arn:aws:iam::${var.aws_account_id}:root"
          }
          Action   = "kms:*"
          Resource = "*"
        },
        {
          Sid    = "Allow S3 Service to Use Key"
          Effect = "Allow"
          Principal = {
            Service = "s3.amazonaws.com"
          }
          Action = [
            "kms:Decrypt",
            "kms:GenerateDataKey"
          ]
          Resource = "*"
          Condition = {
            StringEquals = {
              "kms:ViaService" = "s3.${var.aws_region}.amazonaws.com"
            }
          }
        }
      ],
      # Conditionally add Lambda role permissions only if roles are provided
      length(var.lambda_role_arns) > 0 ? [
        {
          Sid    = "Allow Lambda Functions to Encrypt and Decrypt"
          Effect = "Allow"
          Principal = {
            AWS = var.lambda_role_arns
          }
          Action = [
            "kms:Decrypt",
            "kms:Encrypt",
            "kms:GenerateDataKey",
            "kms:GenerateDataKeyWithoutPlaintext",
            "kms:DescribeKey"
          ]
          Resource = "*"
        }
      ] : []
    )
  })
}

