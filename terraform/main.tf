# ---------------------------------------------------------------------------
# Terraform — Demonstration S3 bucket
# This is OPTIONAL and NOT required for LocalStack or moto tests.
# ---------------------------------------------------------------------------

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Random suffix to avoid global bucket-name collisions
resource "random_id" "suffix" {
  byte_length = 4
}

resource "aws_s3_bucket" "demo" {
  bucket = "${var.bucket_prefix}-${random_id.suffix.hex}"

  tags = {
    Environment = "demo"
    ManagedBy   = "terraform"
    Project     = "aws-testkit"
  }
}

resource "aws_s3_bucket_versioning" "demo" {
  bucket = aws_s3_bucket.demo.id

  versioning_configuration {
    status = "Enabled"
  }
}

# ---------------------------------------------------------------------------
# (Optional) GitHub OIDC provider + IAM role skeleton
# Uncomment and fill in your GitHub org/repo to use with GitHub Actions OIDC.
# Do NOT hard-code AWS account IDs — use data sources or variables.
# ---------------------------------------------------------------------------

# data "aws_caller_identity" "current" {}
#
# resource "aws_iam_openid_connect_provider" "github" {
#   url             = "https://token.actions.githubusercontent.com"
#   client_id_list  = ["sts.amazonaws.com"]
#   thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
# }
#
# resource "aws_iam_role" "github_actions" {
#   name = "github-actions-aws-testkit"
#
#   assume_role_policy = jsonencode({
#     Version = "2012-10-17"
#     Statement = [
#       {
#         Effect = "Allow"
#         Principal = {
#           Federated = aws_iam_openid_connect_provider.github.arn
#         }
#         Action = "sts:AssumeRoleWithWebIdentity"
#         Condition = {
#           StringEquals = {
#             "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
#           }
#           StringLike = {
#             # Replace with your org/repo
#             "token.actions.githubusercontent.com:sub" = "repo:YOUR_ORG/YOUR_REPO:*"
#           }
#         }
#       }
#     ]
#   })
# }
#
# resource "aws_iam_role_policy_attachment" "github_actions_s3" {
#   role       = aws_iam_role.github_actions.name
#   policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
# }
