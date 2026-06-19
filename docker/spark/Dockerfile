FROM bitnami/spark:3.4.1

USER root

RUN pip install --no-cache-dir pyyaml==6.0.1 google-cloud-storage==2.14.0 delta-spark==2.4.0 loguru great-expectations==0.18.0 boto3 azure-storage-blob

USER 1001
