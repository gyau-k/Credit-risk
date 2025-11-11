# To create individual statefiles for workspaces
terraform {
  backend "s3" {
    bucket         = "s3statebackend-credit-risk-011"
    key            = "terraform.tfstate"
    region         = "eu-west-1"
    dynamodb_table = "state-lock"
    encrypt        = true
  }
}