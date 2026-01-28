module "s3" {
    source = "./modules/s3"
}

module "iam" {
    source = "./modules/iam"
    bucket_s3_etl_arn = module.s3.root_path_bucket_s3_flights
}

module "aws_lambda_extract" {
    source = "./modules/lambda"
    lambda_extract_name = "lambda_extract_api"
    lambda_execution_role_flights_arn = module.iam.lambda_arn_etl_flights
    ecr_repository_url = var.ecr_repository_url
    image_tag = var.image_tag
    command_aws_lambda_flights = "historical_flights_airport_gym.conectores.anac.lambda_handler.lambda_handler"

}

module "aws_lambda_data_quality_staging_bronze" {
    source = "./modules/lambda"
    lambda_extract_name = "lambda_data_quality_staging_bronze"
    lambda_execution_role_flights_arn = module.iam.lambda_arn_etl_flights
    ecr_repository_url = var.ecr_repository_url
    image_tag = var.image_tag
    command_aws_lambda_flights = "historical_flights_airport_gym.soda.check_function.lambda_handler"

}
