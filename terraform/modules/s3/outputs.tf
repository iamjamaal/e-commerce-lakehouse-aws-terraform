output "raw_bucket_name" {
  value = aws_s3_bucket.raw.id
}

output "raw_bucket_arn" {
  value = aws_s3_bucket.raw.arn
}

output "dwh_bucket_name" {
  value = aws_s3_bucket.dwh.id
}

output "dwh_bucket_arn" {
  value = aws_s3_bucket.dwh.arn
}

output "archived_bucket_name" {
  value = aws_s3_bucket.archived.id
}

output "archived_bucket_arn" {
  value = aws_s3_bucket.archived.arn
}

output "rejected_bucket_name" {
  value = aws_s3_bucket.rejected.id
}

output "rejected_bucket_arn" {
  value = aws_s3_bucket.rejected.arn
}

output "scripts_bucket_name" {
  value = aws_s3_bucket.scripts.id
}

output "scripts_bucket_arn" {
  value = aws_s3_bucket.scripts.arn
}

output "athena_results_bucket_name" {
  value = aws_s3_bucket.athena_results.id
}

output "athena_results_bucket_arn" {
  value = aws_s3_bucket.athena_results.arn
}
