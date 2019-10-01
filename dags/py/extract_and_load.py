from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.hooks.postgres_hook import PostgresHook

from google.cloud import storage
from datetime import datetime
from io import BytesIO
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'keys/airy-media-254122-973505938453.json'

# creates postgres table if none
def create_table(**kwargs):
    conn = PostgresHook(postgres_conn_id='my_local_db').get_conn()
    cursor = conn.cursor()
    with open('sql/create_postgres_table.sql', 'r') as f:
        cursor.execute(f.read())

    conn.commit()
    conn.close()
    cursor.close()

# loads postgres table
def load_table(**kwargs):
    conn = PostgresHook(postgres_conn_id='my_local_db').get_conn()
    cursor = conn.cursor()
    client = storage.Client()
    bucket = client.get_bucket('airy-media-254122.appspot.com')
    blob = bucket.get_blob('bq_bucket/date_range.csv')
    csv = blob.download_as_string().decode('utf-8').split('\n')
    rows = [row.split(',') for row in csv]
    insert = "INSERT INTO austin_service_reports VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}');"
    for row in rows:
        query = insert.format(
            row[0],row[1],row[2],row[3],row[4],
            str(datetime.fromtimestamp(float(row[5]))),
            str(datetime.fromtimestamp(float(row[6])))
        )
        cursor.execute(query)
        conn.commit()
    
    conn.commit()
    conn.close()
    cursor.close()

# decides to either load or create table
def branch_task(**kwargs):
    conn = PostgresHook(postgres_conn_id='my_local_db').get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT 1 FROM austin_service_reports;')
        return "load_table_task"
    except:
        return 'create_table_task'

# collects values based off date range from bigquery and pushes to gcs as csv
def bq_hook(**kwargs):
    prev_ds = kwargs['prev_ds']
    ds = kwargs['ds']
    # print(kwargs)
    hook = BigQueryHook(
        bigquery_conn_id='my_gcp_connection',
        use_legacy_sql=False
    )
    conn = hook.get_conn()
    cursor = conn.cursor()
    with open('sql/query_bq_dataset.sql', 'r') as f:
        query = f.read()
        query = query.format(prev_ds,ds)

    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    cursor.close()

    client = storage.Client()
    bucket = client.get_bucket('airy-media-254122.appspot.com')
    file_blob = bucket.blob('bq_bucket/date_range.csv')

    results = '\n'.join([','.join([str(val) for val in row]) for row in results])
    file_blob.upload_from_string(results)
    