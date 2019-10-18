from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta

from transfer_data_pipeline.py.transfer_data import average_days_open, create_temp_table


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2014, 1, 2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'transfer_data',
    catchup=False,
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1
)

task_calculate_avg_days = PythonOperator(
    task_id='task_calculate_avg_days',
    python_callable=average_days_open,
    provide_context=True,
    dag=dag
)

task_create_temp_table = PythonOperator(
    task_id='task_create_temp_table',
    python_callable=create_temp_table,
    provide_context=True,
    dag=dag
)

task_calculate_avg_days >> task_create_temp_table