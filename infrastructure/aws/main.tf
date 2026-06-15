# AWS Infrastructure - Enterprise Data Platform

# 1. Scalable Storage (Medallion Layers)
resource "aws_s3_bucket" "medallion_storage" {
  for_each = toset(["raw", "bronze", "silver", "gold"])
  bucket   = "enterprise-datalake-${each.key}-${var.environment}"

  tags = {
    Layer       = each.key
    Environment = var.environment
    Platform    = "DataEngineering"
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

# 2. Managed Compute (Glue Spark Jobs)
resource "aws_iam_role" "glue_service_role" {
  name = "EnterpriseGlueRole-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_attachment" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_glue_job" "medallion_processing" {
  name     = "enterprise-medallion-pipeline"
  role_arn = aws_iam_role.glue_service_role.arn

  command {
    script_location = "s3://${aws_s3_bucket.medallion_storage["raw"].id}/scripts/main_job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"        = "python"
    "--continuous-log-logGroup" = "/aws-glue/jobs/enterprise-data-pipeline"
    "--enable-metrics"      = "true"
  }

  max_retries = 2
  timeout     = 2880
}

# 3. Secret Management
resource "aws_secretsmanager_secret" "database_credentials" {
  name = "enterprise/data-platform/${var.environment}/db-creds"
  description = "Managed database credentials for ingestion"
}

# 4. Observability
resource "aws_cloudwatch_log_group" "pipeline_logs" {
  name              = "/enterprise/data-platform/pipeline-execution"
  retention_in_days = 90
}
