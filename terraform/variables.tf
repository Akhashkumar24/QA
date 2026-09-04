variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "bucket_prefix" {
  description = "Prefix for the demo S3 bucket name (a random suffix is appended)"
  type        = string
  default     = "aws-testkit-demo"
}
