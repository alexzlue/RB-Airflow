from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_get_data import BigQueryGetDataOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

import json
from datetime import datetime, timedelta

from py.extract_and_load import load_table, create_table, branch_task, bq_hook


default_args = {
    'owner': 'airflow',
    'ignore_first_depends_on_past': True,
    'start_date': datetime(2014, 1, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# create dag and schedule a load interval every day at midnight (7am UTC)
dag = DAG('bigquery', default_args=default_args, 
          schedule_interval=timedelta(days=1))

'''
# extracts bq to a gcs bucket as csv
task_bq_to_gcs = BigQueryToCloudStorageOperator(
    source_project_dataset_table='bigquery-public-data.austin_311.311_service_requests',
    destination_cloud_storage_uris=['gs://airy-media-254122.appspot.com/bq_bucket/austin_311_service_requests.csv'],
    bigquery_conn_id='my_gcp_connection',
    task_id='bq_to_gcs',
    dag=dag
)
'''

# branch to either load or create table
branch_task = BranchPythonOperator(
    task_id='branch_task',
    provide_context=True,
    python_callable=branch_task,
    dag=dag
)

# loads postgres table from csv
task_gcs_to_postgres = PythonOperator(
    task_id='load_table_task',
    python_callable=load_table,
    provide_context=True,
    dag=dag
)

# creates table
task_create = PythonOperator(
    task_id='create_table_task',
    python_callable=create_table,
    provide_context=True,
    dag=dag
)

# extracts bq to a gcs bucket as csv
task_bq_to_gcs = PythonOperator(
    task_id='task_bq_to_gcs',
    python_callable=bq_hook,
    provide_context=True,
    dag=dag
)


task_bq_to_gcs >> branch_task
branch_task >> [task_create, task_gcs_to_postgres]
task_create >> task_gcs_to_postgres