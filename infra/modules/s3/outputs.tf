output "root_path_bucket_s3_flights" {
    description = "Saida de ARN para utilizacao em politicas do IAM"
    value = aws_s3_bucket.bucket_etl_flights.arn

}
