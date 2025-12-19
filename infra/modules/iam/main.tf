# 1. ------------------ Role to Lambda ---------------------------

resource "aws_iam_role" "role_lambda_flights" {
    name=var.iam_role_name_lambda

    assume_role_policy = jsonencode({
        version="2012-10-17"
        Statement = [
            {
                Action = "sts:AssumeRole",
                Effect = "Allow",
                Principal = {
                    Service = "lambda.amazonaws.com"
                }
            }
        ]
    })
}

#2. -------------- Lambda Policies ------------------------------

resource "aws_iam_police" "policy_get_access_s3" {
    name = var.name_policy_get_access_s3

    policy = jsonencode({
        Version = "2012-10-17"
        Statement = [
            {
                Action = [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject"
                ]
                Effect = "Allow"
                Resource = var.bucket_s3_etl_arn
            }
        ]
    })
}


#3. -------------- Lambda Attachment -----------------------------
resource "aws_iam_role_policy_attachment" "attach_cloudwatch_logs_policy" {
    role = aws_iam_role.role_lambda_flights.name
    policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "attach_s3_access_policy" {
    role = aws_iam_role.role_lambda_flights.name
    policy_arn = aws_iam_police.policy_get_access_s3.arn
}
