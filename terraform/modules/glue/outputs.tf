output "products_job_name" {
  description = "Name of the Glue ETL job for products"
  value       = aws_glue_job.products.name
}

output "orders_job_name" {
  description = "Name of the Glue ETL job for orders"
  value       = aws_glue_job.orders.name
}

output "order_items_job_name" {
  description = "Name of the Glue ETL job for order items"
  value       = aws_glue_job.order_items.name
}

output "crawler_name" {
  description = "Name of the Glue Crawler for DWH Delta tables"
  value       = aws_glue_crawler.dwh.name
}

output "database_name" {
  description = "Name of the Glue Data Catalog database"
  value       = aws_glue_catalog_database.lakehouse.name
}
