from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

from datetime import datetime, timedelta

from transfer_data_pipeline.py.transfer_data import load_transfer

SQL_PATH = './transfer_data_pipeline/sql/'

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

task_create_tables = PostgresOperator(
    task_id='task_create_tables',
    sql=SQL_PATH + 'create_transfer_and_aggregate_tables.sql',
    postgres_conn_id='my_local_db',
    dag=dag
)

task_create_views = PostgresOperator(
    task_id='task_create_views',
    sql=SQL_PATH + 'create_views.sql',
    postgres_conn_id='my_local_db',
    dag=dag
)

task_load_transfer_table = PythonOperator(
    task_id='task_load_transfer_table',
    python_callable=load_transfer,
    provide_context=True,
    dag=dag
)

task_transfer_to_aggregate_table = PostgresOperator(
    task_id='task_transfer_to_aggregate_table',
    sql=SQL_PATH + 'insert_into_aggregate.sql',
    postgres_conn_id='my_local_db',
    dag=dag
)

task_create_tables.set_downstream(task_create_views)
task_create_views.set_downstream(task_load_transfer_table)
task_load_transfer_table.set_downstream(task_transfer_to_aggregate_table)