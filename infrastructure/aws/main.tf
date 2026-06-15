# AWS Cloud Infrastructure - Enterprise Data Platform

# 1. Multi-layer Data Lake Storage
resource "aws_s3_bucket" "medallion_storage" {
  for_each = toset(["raw", "bronze", "silver", "gold"])
  bucket   = "enterprise-datalake-${each.key}-${var.environment}"

  tags = {
    Platform    = "DataEngineering"
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

resource "aws_s3_bucket_versioning" "storage_versioning" {
  for_each = aws_s3_bucket.medallion_storage
  bucket   = each.value.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "storage_encryption" {
  for_each = aws_s3_bucket.medallion_storage
  bucket   = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# 2. Serverless Spark Compute (Glue)
resource "aws_iam_role" "glue_service_role" {
  name = "EnterpriseGlueExecutionRole-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_policy" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_glue_job" "batch_processing_job" {
  name     = "enterprise-medallion-pipeline"
  role_arn = aws_iam_role.glue_service_role.arn

  command {
    script_location = "s3://${aws_s3_bucket.medallion_storage["raw"].id}/scripts/main_job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"            = "python"
    "--continuous-log-logGroup" = "/aws-glue/jobs/enterprise-medallion"
    "--enable-metrics"          = "true"
  }

  max_retries = 1
  timeout     = 1440
}

# 3. Secure Secret Storage
resource "aws_secretsmanager_secret" "ingestion_secrets" {
  name        = "enterprise/data-platform/${var.environment}/credentials"
  description = "Managed secrets for source database connections"
}

# 4. Centralized Observability
resource "aws_cloudwatch_log_group" "processing_logs" {
  name              = "/enterprise/data-platform/execution"
  retention_in_days = 60
}
