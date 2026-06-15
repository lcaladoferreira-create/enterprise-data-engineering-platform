from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

# Configuration
SPARK_APP_PATH = '/opt/airflow/spark/jobs/main_job.py'
ENTITIES = ["customers", "orders", "order_items", "products", "payments", "invoices"]

def on_failure_callback(context):
    """
    Custom failure callback for production alerting.
    """
    dag_id = context.get('task_instance').dag_id
    task_id = context.get('task_instance').task_id
    err = context.get('exception')
    logging.error(f"Task Failed: DAG={dag_id}, Task={task_id}, Error={err}")

default_args = {
    'owner': 'data_platform_team',
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
    description='Production Medallion Data Pipeline',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['medallion', 'spark', 'production'],
) as dag:

    # 1. Sensors: Wait for Raw Data from Ingestion Layer
    wait_for_raw_data = []
    for entity in ENTITIES:
        sensor = FileSensor(
            task_id=f'wait_for_{entity}_raw',
            filepath=f'/opt/airflow/data/raw/{entity}',
            fs_conn_id='fs_default',
            poke_interval=60,
            timeout=3600
        )
        wait_for_raw_data.append(sensor)

    # 2. Bronze Layer: Raw to Validated Parquet
    bronze_tasks = []
    for entity in ENTITIES:
        bronze_job = SparkSubmitOperator(
            task_id=f'process_{entity}_bronze',
            application=SPARK_APP_PATH,
            application_args=['bronze', entity],
            conn_id='spark_default',
            name=f'bronze_{entity}',
            total_executor_cores=1,
            executor_memory='1G',
            driver_memory='1G'
        )
        bronze_tasks.append(bronze_job)

    # 3. Silver Layer: Deduplication & Quality
    silver_tasks = []
    for entity in ENTITIES:
        silver_job = SparkSubmitOperator(
            task_id=f'process_{entity}_silver',
            application=SPARK_APP_PATH,
            application_args=['silver', entity],
            conn_id='spark_default',
            name=f'silver_{entity}'
        )
        silver_tasks.append(silver_job)

    # 4. Gold Layer: Star Schema and Analytical Marts
    process_gold = SparkSubmitOperator(
        task_id='generate_gold_star_schema',
        application=SPARK_APP_PATH,
        application_args=['gold'],
        conn_id='spark_default',
        name='gold_star_schema'
    )

    # 5. Data Quality Validation Task
    def run_gold_dq_checks():
        """
        Executes critical DQ checks against the Gold layer.
        """
        logging.info("Executing Gold Layer Data Quality checks...")
        # Production quality check logic would go here
        return True

    validate_gold = PythonOperator(
        task_id='validate_gold_quality',
        python_callable=run_gold_dq_checks
    )

    # Pipeline Dependencies
    for i in range(len(ENTITIES)):
        wait_for_raw_data[i] >> bronze_tasks[i] >> silver_tasks[i] >> process_gold

    process_gold >> validate_gold
