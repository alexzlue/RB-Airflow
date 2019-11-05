from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors import CheckDBSensor, CheckGCSFileSensor, CheckBacklogSensor

from datetime import datetime, timedelta

from extract_load_pipeline.py.extract_and_load import load_table, bq_to_gcs


default_args = {
    'owner': 'airflow',
    'ignore_first_depends_on_past': True,
    'start_date': datetime(2014, 1, 2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# create dag and schedule a load interval every day at midnight (7am UTC)
dag = DAG(
    'extract_and_load',
    catchup=False,
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    max_active_runs=1
)

# task to create table if it does not exist
task_create_table = PostgresOperator(
    task_id='task_create_table',
    sql='./extract_load_pipeline/sql/create_postgres_table.sql',
    postgres_conn_id='my_local_db',
    dag=dag
)

# extracts bq to a gcs bucket as csv
task_bq_to_gcs = PythonOperator(
    task_id='task_bq_to_gcs',
    python_callable=bq_to_gcs,
    provide_context=True,
    op_kwargs={'start_date': default_args['start_date']},
    dag=dag
)

# sensor check to see if gcs file exists
task_check_gcs_file = CheckGCSFileSensor(
    task_id='task_check_gcs_file',
    dag=dag
)

# loads postgres table from csv
task_gcs_to_postgres = PythonOperator(
    task_id='task_gcs_to_postgres',
    python_callable=load_table,
    provide_context=True,
    dag=dag
)

# sensor check to determine data validity
task_check_db = CheckDBSensor(
    task_id='task_check_db',
    pg_conn_id='my_local_db',
    bq_conn_id='my_gcp_connection',
    dag=dag
)

# checks if backlog are only outdated info
task_check_backlog = CheckBacklogSensor(
    task_id='task_check_backlog',
    pg_conn_id='my_local_db',
    dag=dag
)

# triggers load to aggregate table
task_trigger_transform = TriggerDagRunOperator(
    task_id='task_trigger_transfer',
    trigger_dag_id='transfer_data',
    dag=dag
)

task_create_table.set_downstream(task_bq_to_gcs)
task_bq_to_gcs.set_downstream(task_check_gcs_file)
task_check_gcs_file.set_downstream(task_gcs_to_postgres)
task_gcs_to_postgres.set_downstream(task_check_db)
task_check_db.set_downstream(task_check_backlog)
task_check_backlog.set_downstream(task_trigger_transform)
