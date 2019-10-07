from airflow import DAG
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

import json
from datetime import datetime, timedelta

from py.extract_and_load import load_table, bq_to_gcs, create_table


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
dag = DAG('bigquery', default_args=default_args, 
          schedule_interval=timedelta(days=1),
          max_active_runs=1)

# task to create table if it does not exist
task_create_table = PythonOperator(
    task_id='task_create_table',
    python_callable=create_table,
    provide_context=True,
    dag=dag
)

# loads postgres table from csv
task_gcs_to_postgres = PythonOperator(
    task_id='task_gcs_to_postgres',
    python_callable=load_table,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    provide_context=True,
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

task_create_table >> task_bq_to_gcs
task_bq_to_gcs >> task_gcs_to_postgres
