resource "aws_lambda_function" "lambda_extract_flights" {
    function_name = var.lambda_extract_name
    package_type = "Image"
    role = var.lambda_execution_role_flights_arn
    image_uri = "${var.ecr_repository_url}:{var.image_tag}"

    image_config {
        command = [var.command_aws_lambda_flights]
    }

    memory_size = 2048
    timeout = 360

    logging_config {
        log_format = "JSON"
    }
}
