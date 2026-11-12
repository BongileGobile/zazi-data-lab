# WHY: Configures Terraform to use the official AWS provider while routing 
# all API calls to our local open-source LocalStack container (zero AWS cost).
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region                      = "af-south-1" # WHY: Cape Town region alignment for South African data residency.
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  s3_use_path_style           = true # WHY: Forces path-style URLs (localhost:4566/bucket) so LocalStack resolves requests immediately without custom DNS.

  # WHY: Directs Terraform S3 operations to our local LocalStack endpoint instead of live AWS.
  endpoints {
    s3 = "http://localhost:4566"
  }
}

# WHY: Provisions the analytical data lake bucket via Infrastructure-as-Code (IaC)
# so storage infrastructure is version-controlled and reproducible across environments.
resource "aws_s3_bucket" "zazi_data_lake" {
  bucket        = "zazi-data-lake-prod"
  force_destroy = true # WHY: Allows clean teardown in local lab environments without manual bucket emptying.
}

# WHY: Enforces versioning on the data lake to protect historical Parquet files 
# from accidental overwrites or corruption.
resource "aws_s3_bucket_versioning" "zazi_lake_versioning" {
  bucket = aws_s3_bucket.zazi_data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}
