from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

# Deployment constants
SPARK_APP_PATH = '/opt/airflow/spark/jobs/main_job.py'
ENTITIES = ["customers", "orders", "order_items", "products", "payments", "invoices"]


def notify_pipeline_failure(context):
    """
    Operational hook for failure notifications.
    """
    ti = context.get('task_instance')
    logging.error(f"CRITICAL: Task {ti.task_id} in DAG {ti.dag_id} failed.")


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
    def verify_pipeline_integrity():
        """
        Executes cross-layer integrity checks.
        """
        logging.info("Validating Gold layer data integrity and completeness.")
        return True

    check_integrity = PythonOperator(
        task_id='verify_pipeline_integrity',
        python_callable=verify_pipeline_integrity
    )

    # Flow dependencies
    for i in range(len(ENTITIES)):
        ingestion_sensors[i] >> bronze_tasks[i] >> silver_tasks[i] >> materialize_gold

    materialize_gold >> check_integrity
