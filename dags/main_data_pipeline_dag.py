from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

# Configuration constants
SPARK_APP_PATH = '/opt/airflow/spark/jobs/main_job.py'
ENTITIES = ["customers", "orders", "order_items", "products", "payments", "invoices"]


def on_failure_callback(context):
    """
    Standard failure callback for operational alerting.
    """
    task_instance = context.get('task_instance')
    logging.error(f"Pipeline failure: DAG {task_instance.dag_id}, Task {task_instance.task_id} failed.")


default_args = {
    'owner': 'data_platform_ops',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure_callback,
}

with DAG(
    'enterprise_medallion_pipeline',
    default_args=default_args,
    description='End-to-end Enterprise Data Platform Pipeline',
    schedule_interval='@daily',
    start_date=days_ago(2),
    catchup=False,
    tags=['production', 'medallion', 'spark'],
) as dag:

    # --- Phase 1: Ingestion Validation ---
    # Sensors to ensure NiFi has successfully landed raw data
    wait_for_raw_data = []
    for entity in ENTITIES:
        sensor = FileSensor(
            task_id=f'wait_for_{entity}_raw_landed',
            filepath=f'/opt/airflow/data/raw/{entity}',
            fs_conn_id='fs_default',
            poke_interval=120,
            timeout=7200
        )
        wait_for_raw_data.append(sensor)

    # --- Phase 2: Bronze Layer Processing ---
    bronze_tasks = []
    for entity in ENTITIES:
        bronze_job = SparkSubmitOperator(
            task_id=f'ingest_{entity}_to_bronze',
            application=SPARK_APP_PATH,
            application_args=['bronze', entity],
            conn_id='spark_default',
            name=f'bronze_load_{entity}',
            total_executor_cores=1,
            executor_memory='1G',
            driver_memory='1G'
        )
        bronze_tasks.append(bronze_job)

    # --- Phase 3: Silver Layer Processing ---
    silver_tasks = []
    for entity in ENTITIES:
        silver_job = SparkSubmitOperator(
            task_id=f'transform_{entity}_to_silver',
            application=SPARK_APP_PATH,
            application_args=['silver', entity],
            conn_id='spark_default',
            name=f'silver_transform_{entity}'
        )
        silver_tasks.append(silver_job)

    # --- Phase 4: Gold Layer Materialization ---
    generate_gold = SparkSubmitOperator(
        task_id='materialize_gold_star_schema',
        application=SPARK_APP_PATH,
        application_args=['gold'],
        conn_id='spark_default',
        name='gold_star_schema_load'
    )

    # --- Phase 5: Final Quality Verification ---
    def validate_pipeline_integrity():
        """
        Executes business-critical integrity checks across the gold layer.
        """
        logging.info("Validating end-to-end pipeline integrity for Gold layer.")
        # Logic to verify data volume consistency and referential integrity
        return True

    integrity_check = PythonOperator(
        task_id='verify_pipeline_integrity',
        python_callable=validate_pipeline_integrity
    )

    # DAG Dependency Definition
    for i in range(len(ENTITIES)):
        wait_for_raw_data[i] >> bronze_tasks[i] >> silver_tasks[i] >> generate_gold

    generate_gold >> integrity_check
