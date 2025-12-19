terraform {
    backend "s3" {
        bucket = "mateusalmeida-us-east-1-terraform-statefile-with-lock"
        key = "flights-etl-lambda/statefile.tfstate"
        region = "us-east-1"
        encrypt = true
        use_lockfile = true

    }
}
