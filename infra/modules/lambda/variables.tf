variable "lambda_extract_name" {
    description = "Nome da funcao lambda sera enviado pelo main da Infra"
    type = string
}

variable "lambda_execution_role_flights_arn" {
    description = "ARN que sera enviado pelo modulo do IAM"
    type = string
}

variable "ecr_repository_url" {
    description = "URI do repositorio passado pelo GithubActions"
    type = string

}

variable "image_tag" {
    description = "Variavel definida pelo GithubActions"
    type = string
}

variable "command_aws_lambda_flights" {
    description = "Comando de inicializacao da Lambda"
    type = string
}
