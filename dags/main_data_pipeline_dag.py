import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor

# Constants
SPARK_APP_PATH = "/opt/bitnami/spark/spark-apps/jobs/main_job.py"
ENTITIES = ["customers", "orders", "products", "order_items", "payments", "invoices"]


def notify_pipeline_failure(context):
    """Placeholder for slack/email notification."""
    task_instance = context["task_instance"]
    logging.error(f"Task Failed: {task_instance.task_id} in DAG {task_instance.dag_id}")


def run_dq_validation(layer: str, entity: str, suite_name: str) -> None:
    """
    Runs Great Expectations data quality validation using DataQualityCheckpoint.
    """
    # Add project root to sys.path to find data_quality module if needed
    sys.path.append(os.environ.get("AIRFLOW_HOME", "/opt/airflow"))

    from pyspark.sql import SparkSession

    from data_quality.checkpoint import DataQualityCheckpoint

    spark = SparkSession.builder.appName(f"DQ_Validation_{layer}_{entity}").getOrCreate()
    path = f"/opt/airflow/data/{layer}/{entity}"

    logging.info(f"Loading data for DQ validation from: {path}")
    if not os.path.exists(path):
        logging.warning(f"Path {path} does not exist. Skipping validation.")
        return

    # Use Delta format for reading
    df = spark.read.format("delta").load(path)
    checkpoint = DataQualityCheckpoint(suite_name)
    checkpoint.validate(df)


default_args = {
    "owner": "data_eng_team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_pipeline_failure,
}

with DAG(
    "enterprise_medallion_pipeline",
    default_args=default_args,
    description="End-to-end Enterprise Data Platform Workflow",
    schedule="@daily",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["medallion", "spark", "v3"],
) as dag:
    # 1. Ingestion Sensors: Verify data landing from NiFi
    ingestion_sensors = []
    for entity in ENTITIES:
        sensor = FileSensor(
            task_id=f"sense_{entity}_arrival",
            filepath=f"/opt/airflow/data/raw/{entity}",
            fs_conn_id="fs_default",
            poke_interval=120,
            timeout=7200,
        )
        ingestion_sensors.append(sensor)

    # 2. Bronze Tasks and DQ
    bronze_tasks = []
    for entity in ENTITIES:
        load_bronze = SparkSubmitOperator(
            task_id=f"load_{entity}_bronze",
            application=SPARK_APP_PATH,
            application_args=["bronze", entity],
            conn_id="spark_default",
            name=f"bronze_load_{entity}",
            total_executor_cores=1,
            executor_memory="1G",
        )

        dq_bronze = PythonOperator(
            task_id=f"validate_{entity}_bronze",
            python_callable=run_dq_validation,
            op_kwargs={"layer": "bronze", "entity": entity, "suite_name": "bronze_suite"},
        )

        load_bronze >> dq_bronze
        bronze_tasks.append(dq_bronze)

    # 3. Silver Tasks and DQ
    silver_tasks = []
    for i, entity in enumerate(ENTITIES):
        clean_silver = SparkSubmitOperator(
            task_id=f"clean_{entity}_silver",
            application=SPARK_APP_PATH,
            application_args=["silver", entity],
            conn_id="spark_default",
            name=f"silver_clean_{entity}",
        )

        dq_silver = PythonOperator(
            task_id=f"validate_{entity}_silver",
            python_callable=run_dq_validation,
            op_kwargs={"layer": "silver", "entity": entity, "suite_name": "silver_suite"},
        )

        bronze_tasks[i] >> clean_silver >> dq_silver
        silver_tasks.append(dq_silver)

    # 4. Gold Task: Dimension and Fact materialization
    materialize_gold = SparkSubmitOperator(
        task_id="materialize_gold_star_schema",
        application=SPARK_APP_PATH,
        application_args=["gold"],
        conn_id="spark_default",
        name="gold_star_schema_materialization",
    )

    # 5. Integrity Verification: Cross-layer validation
    check_integrity = SparkSubmitOperator(
        task_id="verify_pipeline_integrity",
        application=SPARK_APP_PATH,
        application_args=["integrity_check"],
        conn_id="spark_default",
        name="pipeline_integrity_check",
    )

    # Flow dependencies
    for sensor in ingestion_sensors:
        # Each sensor triggers its respective bronze load (matching index)
        entity = sensor.task_id.replace("sense_", "").replace("_arrival", "")
        sensor >> dag.get_task(f"load_{entity}_bronze")

    for st in silver_tasks:
        st >> materialize_gold

    materialize_gold >> check_integrity
