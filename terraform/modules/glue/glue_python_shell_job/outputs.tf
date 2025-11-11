output "job_name" {
  description = "Name of the Glue job"
  value       = aws_glue_job.python_shell_job.name
}

output "job_arn" {
  description = "ARN of the Glue job"
  value       = aws_glue_job.python_shell_job.arn
}

output "job_id" {
  description = "ID of the Glue job"
  value       = aws_glue_job.python_shell_job.id
}

output "role_arn" {
  description = "ARN of the Glue job IAM role"
  value       = aws_iam_role.glue_job_role.arn
}

output "script_s3_location" {
  description = "S3 location of the uploaded script"
  value       = "s3://${var.scripts_bucket}/${var.scripts_prefix}${basename(var.script_path)}"
}
