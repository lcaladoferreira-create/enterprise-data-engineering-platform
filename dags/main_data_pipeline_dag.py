from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging
import glob
import pandas as pd

# Deployment constants
SPARK_APP_PATH = '/opt/airflow/spark/jobs/main_job.py'
ENTITIES = ["customers", "orders", "order_items", "products", "payments", "invoices"]


def notify_pipeline_failure(context):
    """
    Operational hook for failure notifications.
    """
    ti = context.get('task_instance')
    logging.error(f"CRITICAL: Task {ti.task_id} in DAG {ti.dag_id} failed.")


def verify_pipeline_integrity(**kwargs):
    """
    Reads record counts from silver and gold layers and validates consistency.
    Raises AirflowException if critical thresholds are not met.
    """
    logging.info("Starting production integrity verification.")

    data_root = "/opt/airflow/data"

    try:
        # Example for the core entity: Orders
        silver_orders_path = f"{data_root}/silver/orders"
        gold_fact_orders_path = f"{data_root}/gold/fact_orders"

        def get_count(path):
            files = glob.glob(f"{path}/**/*.parquet", recursive=True)
            if not files:
                return 0
            count = 0
            for f in files:
                count += len(pd.read_parquet(f, columns=[]))
            return count

        silver_count = get_count(silver_orders_path)
        gold_count = get_count(gold_fact_orders_path)

        logging.info(f"Integrity Check: Silver Orders = {silver_count}, Gold Fact Orders = {gold_count}")

        if gold_count == 0:
            raise AirflowException("CRITICAL: Gold layer is empty.")

        if gold_count < (silver_count * 0.95):
            raise AirflowException(
                f"CRITICAL: Data loss detected. Gold count {gold_count} is less than 95% of Silver count {silver_count}."
            )

        logging.info("Integrity check passed.")
        return True

    except Exception as e:
        logging.error(f"Integrity check failed: {str(e)}")
        raise AirflowException(f"Pipeline integrity violation: {str(e)}")


default_args = {
    'owner': 'data_eng_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': notify_pipeline_failure,
}

with DAG(
    'enterprise_medallion_pipeline',
    default_args=default_args,
    description='End-to-end Enterprise Data Platform Workflow',
    schedule_interval='@daily',
    start_date=days_ago(2),
    catchup=False,
    tags=['medallion', 'spark', 'v2'],
) as dag:

    # 1. Ingestion Sensors: Verify data landing from NiFi
    ingestion_sensors = []
    for entity in ENTITIES:
        sensor = FileSensor(
            task_id=f'sense_{entity}_arrival',
            filepath=f'/opt/airflow/data/raw/{entity}',
            fs_conn_id='fs_default',
            poke_interval=120,
            timeout=7200
        )
        ingestion_sensors.append(sensor)

    # 2. Bronze Tasks: Load validated raw data
    bronze_tasks = []
    for entity in ENTITIES:
        task = SparkSubmitOperator(
            task_id=f'load_{entity}_bronze',
            application=SPARK_APP_PATH,
            application_args=['bronze', entity],
            conn_id='spark_default',
            name=f'bronze_load_{entity}',
            total_executor_cores=1,
            executor_memory='1G'
        )
        bronze_tasks.append(task)

    # 3. Silver Tasks: Deduplication and Quality
    silver_tasks = []
    for entity in ENTITIES:
        task = SparkSubmitOperator(
            task_id=f'clean_{entity}_silver',
            application=SPARK_APP_PATH,
            application_args=['silver', entity],
            conn_id='spark_default',
            name=f'silver_clean_{entity}'
        )
        silver_tasks.append(task)

    # 4. Gold Task: Dimension and Fact materialization
    materialize_gold = SparkSubmitOperator(
        task_id='materialize_gold_star_schema',
        application=SPARK_APP_PATH,
        application_args=['gold'],
        conn_id='spark_default',
        name='gold_star_schema_materialization'
    )

    # 5. Integrity Verification: Cross-layer validation
    check_integrity = PythonOperator(
        task_id='verify_pipeline_integrity',
        python_callable=verify_pipeline_integrity
    )

    # Flow dependencies
    for i in range(len(ENTITIES)):
        ingestion_sensors[i] >> bronze_tasks[i] >> silver_tasks[i] >> materialize_gold

    materialize_gold >> check_integrity
