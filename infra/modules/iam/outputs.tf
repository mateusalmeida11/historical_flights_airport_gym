output "lambda_arn_etl_flights" {
    description = "ARN de politica que sera anexada a lambda"
    value = aws_iam_role.role_lambda_flights.arn
}
