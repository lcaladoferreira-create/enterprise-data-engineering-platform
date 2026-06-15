# AWS Infrastructure for Data Engineering Platform

provider "aws" {
  region = var.aws_region
}

# 1. S3 Data Lake Buckets (Medallion Architecture)
resource "aws_s3_bucket" "datalake" {
  for_each = toset(["raw", "bronze", "silver", "gold"])
  bucket   = "enterprise-datalake-${each.key}-${var.environment}"
}

resource "aws_s3_bucket_public_access_block" "datalake_lockdown" {
  for_each = aws_s3_bucket.datalake
  bucket   = each.value.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# 2. IAM Role for Spark Processing (Glue/EMR)
resource "aws_iam_role" "spark_role" {
  name = "DataEngineeringSparkRole-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = ["glue.amazonaws.com", "elasticmapreduce.amazonaws.com"]
        }
      }
    ]
  })
}

# 3. AWS Glue Job for Batch Processing
resource "aws_glue_job" "medallion_transformation" {
  name     = "medallion-transformation-job"
  role_arn = aws_iam_role.spark_role.arn

  command {
    script_location = "s3://${aws_s3_bucket.datalake["raw"].id}/scripts/main_job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"        = "python"
    "--continuous-log-logGroup" = "/aws-glue/jobs/medallion-transformation"
    "--enable-metrics"      = "true"
  }
}

# 4. AWS Secrets Manager for DB Credentials
resource "aws_secretsmanager_secret" "db_creds" {
  name = "enterprise/data-platform/db-credentials"
}

# 5. CloudWatch Log Group
resource "aws_cloudwatch_log_group" "pipeline_logs" {
  name              = "/enterprise/data-platform/pipeline"
  retention_in_days = 30
}
