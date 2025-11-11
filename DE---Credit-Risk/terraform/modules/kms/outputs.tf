# KMS Module Outputs

output "kms_key_id" {
  description = "The globally unique identifier for the KMS key"
  value       = aws_kms_key.data_encryption.key_id
}

output "kms_key_arn" {
  description = "The Amazon Resource Name of the KMS key"
  value       = aws_kms_key.data_encryption.arn
}

output "kms_key_alias" {
  description = "The alias of the KMS key"
  value       = aws_kms_alias.data_encryption_alias.name
}

output "kms_key_alias_arn" {
  description = "The ARN of the KMS key alias"
  value       = aws_kms_alias.data_encryption_alias.arn
}

