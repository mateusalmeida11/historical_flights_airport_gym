resource "aws_s3_bucket" "bucket_etl_flights" {
    bucket = "mateus-us-east-1-etl-flights"

}

resource "aws_s3_bucket_public_access_block" "bucket_etl_flights_block" {
    bucket = aws_s3_bucket.bucket_etl_flights.id

    block_public_acls = true
    block_public_policy = true
    ignore_public_acls = true
    restrict_public_buckets = true


}
