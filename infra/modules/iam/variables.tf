variable "iam_role_name_lambda" {
    description = "Nome da Politica de Execucao das Lambdas de Processamento"
    type = string
    default = "lambda_iam_execution_etl_flights"
}

variable "bucket_s3_etl_arn" {
    description = "ARN do Bucket S3 Usado como Storage"
    type = string
}

variable "name_policy_get_access_s3" {
    description = "Politica que garante acesso ao S3"
    type = string
    default = "lambda_policy_access_bucket_s3_flights"
}
