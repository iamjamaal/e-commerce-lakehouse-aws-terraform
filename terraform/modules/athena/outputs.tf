output "workgroup_name" {
  description = "Name of the Athena workgroup"
  value       = aws_athena_workgroup.main.name
}

output "workgroup_arn" {
  value = aws_athena_workgroup.main.arn
}
