resource "aws_ecr_repository" "docker_lambda_flights" {
    name = "mateus-almeida-us-east-1-ecr-flights"
    image_tag_mutability = "MUTABLE"
    image_scanning_configuration {
        scan_on_push = true
    }
}
