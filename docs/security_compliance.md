# Security & Compliance (GDPR/LGPD)

## 1. Identity & Access Management (IAM)
The platform follows the **Principle of Least Privilege**.

*   **Data Engineer Role**: Full access to Raw, Bronze, and Silver; read-only for Gold.
*   **Data Analyst Role**: Read-only access to the Gold layer; no access to Raw/Bronze/Silver.
*   **Service Accounts**: Each Spark job runs with a dedicated service account limited to its specific buckets.

## 2. PII Protection (GDPR)
*   **Identification**: Emails, Phone numbers, and Physical addresses are tagged as PII.
*   **Masking**: These fields are automatically hashed using SHA-256 in the Silver layer.
*   **Access Control**: Unmasked PII is never stored in the Gold layer.

## 3. Data Residency
*   Regional buckets (e.g., `us-east-1` or `europe-west-1`) are used to ensure data remains within specified geographical boundaries.

## 4. Encryption
*   **At Rest**: All S3/GCS buckets are encrypted using Customer Managed Keys (CMK) in KMS.
*   **In Transit**: TLS 1.2+ is enforced for all data movement between NiFi, Spark, and Databases.

## 5. Right to be Forgotten
The platform supports deletion requests:
1.  Deletion request received with `customer_id`.
2.  Spark job runs a "compliance sweep" to remove or anonymize the specific record across all Medallion layers.
3.  Confirmation log is generated (without storing the PII again).
