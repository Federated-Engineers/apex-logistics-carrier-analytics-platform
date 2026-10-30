terraform {
  backend "s3" {
    bucket = "federated-engineers-staging-elite-apex-logistics-tfstate"
    key    = "apex_logistics/staging/terraform.tfstate"
    region = "eu-central-1"
  }
}