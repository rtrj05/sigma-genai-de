from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'data-engineering',
   'retries': 2,
   'retry_delay': timedelta(minutes=5),
    'email_on_failure': True
}

def on_failure_callback(context):
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    error_message = context['exception']
    logging.error(f"Dag ID: {dag_id}, Task ID: {task_id}, Execution Date: {execution_date}, Error: {error_message}")

def sla_miss_callback(context):
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']
    logging.warning(f"Dag ID: {dag_id}, Execution Date: {execution_date}, SLA Miss")

def extract_bronze(**context):
    """Read raw CSV files and write as Bronze Parquet."""
    logging.info(f"Starting Bronze extraction for {context['execution_date']}")
    # CSV reading and Parquet writing logic here
    logging.info(f"Completed Bronze extraction for {context['execution_date']}")
    raise Exception("Simulated failure")  # For testing failure handling

def transform_silver(**context):
    """Transform Bronze data to Silver."""
    logging.info(f"Starting Silver transformation for {context['execution_date']}")
    # Data transformation logic here
    logging.info(f"Completed Silver transformation for {context['execution_date']}")
    raise Exception("Simulated failure")  # For testing failure handling

def build_gold(**context):
    """Build Gold aggregation tables."""
    logging.info(f"Starting Gold build for {context['execution_date']}")
    # Aggregation logic here
    logging.info(f"Completed Gold build for {context['execution_date']}")
    raise Exception("Simulated failure")  # For testing failure handling

with DAG(
    dag_id='sigma_transaction_pipeline',
    schedule='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    on_failure_callback=on_failure_callback,
    sla_miss_callback=sla_miss_callback,
    tags=['sigma', 'transactions', 'daily'],
    description="Daily Bronze->Silver->Gold pipeline for Sigma DataTech transactions"
) as dag:

    extract_bronze_task = PythonOperator(
        task_id='extract_bronze',
        python_callable=extract_bronze,
        on_failure_callback=on_failure_callback
    )

    transform_silver_task = PythonOperator(
        task_id='transform_silver',
        python_callable=transform_silver,
        on_failure_callback=on_failure_callback
    )

    build_gold_task = PythonOperator(
        task_id='build_gold',
        python_callable=build_gold,
        on_failure_callback=on_failure_callback
    )

    extract_bronze_task >> transform_silver_task >> build_gold_task
