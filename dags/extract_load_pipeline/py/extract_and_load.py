from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.hooks.postgres_hook import PostgresHook

from google.cloud import storage
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile
from io import BytesIO
import gcs_client
import os


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'keys/airy-media-254122-973505938453.json'
CRED = gcs_client.Credentials(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
PROJECT = gcs_client.Project('airy-media-254122', CRED)
BUCKET = PROJECT.list()[0]
SQL_PATH = 'dags/extract_load_pipeline/sql/'

# loads postgres table, creates a table if it does not exist
def load_table(**kwargs):
    conn = PostgresHook(postgres_conn_id='my_local_db').get_conn()
    cursor = conn.cursor()

    client = storage.Client()
    bucket = client.get_bucket('airy-media-254122.appspot.com')
    blob = bucket.get_blob('bq_bucket/bq_dataset.csv')

    # create a temporary file and store csv into that to read
    tempf = NamedTemporaryFile()
    blob.download_to_filename(tempf.name)

    query = "COPY airflow.austin_service_reports FROM '"+tempf.name+"' WITH (FORMAT csv)"
    cursor.execute(query)

    tempf.close()
    conn.commit()
    cursor.close()
    conn.close()

# collects values based off date range from bigquery and pushes to gcs as csv
def bq_to_gcs(**kwargs):
    ds = kwargs['ds']
    previous = datetime.strptime(kwargs['prev_ds'], '%Y-%m-%d').date()

    # get the last current date from Postgres
    conn = PostgresHook(postgres_conn_id='my_local_db').get_conn()
    cursor = conn.cursor()

    cursor.execute('SELECT MAX(CAST(created_date AS DATE)) FROM airflow.austin_service_reports;')
    
    recent_ds = cursor.fetchone()[0]
    if recent_ds is not None:
        recent_ds+=timedelta(days=1)
        if recent_ds < previous:
            prev_ds = datetime.strftime(recent_ds, '%Y-%m-%d')
        else:
            prev_ds = kwargs['prev_ds']
    else:
        prev_ds = datetime.strftime(kwargs['start_date']-timedelta(days=1), '%Y-%m-%d')
    
    cursor.close()
    conn.close()

    # open connection to BigQuery
    hook = BigQueryHook(
        bigquery_conn_id='my_gcp_connection',
        use_legacy_sql=False
    )
    conn = hook.get_conn()
    cursor = conn.cursor()
    with open(SQL_PATH + 'query_bq_dataset.sql', 'r') as f:
        query = f.read()
        query = query.format(prev_ds,ds)

    cursor.execute(query)

    # write to gcs bucket
    with BUCKET.open('bq_bucket/bq_dataset.csv', 'w') as f:
        while True:
            result = cursor.fetchone()
            if result is None:
                break
            
            if result[8] is None:
                result[8]= ''
            else:
                result[8] = datetime.utcfromtimestamp(result[8])
            result[7] = datetime.utcfromtimestamp(result[7])
            result[6] = datetime.utcfromtimestamp(result[6])
            f.write(','.join([str(val) for val in result]) + '\n')

    cursor.close()
    conn.close()
