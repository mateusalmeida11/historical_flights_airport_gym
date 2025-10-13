output "ecr_repository_url" {
    description = "URL de saida do repositorio ECR"
    value = aws_ecr_repository.docker_lambda_flights.repository_url
}
