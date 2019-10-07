from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.hooks.postgres_hook import PostgresHook

from google.cloud import storage
from datetime import datetime
from io import BytesIO
import gcs_client
import os


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'keys/airy-media-254122-973505938453.json'
CRED = gcs_client.Credentials(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
PROJECT = gcs_client.Project('airy-media-254122', CRED)
BUCKET = PROJECT.list()[0]

# loads postgres table, creates a table if it does not exist
def load_table(**kwargs):
    conn = PostgresHook(postgres_conn_id='my_local_db').get_conn()
    cursor = conn.cursor()

    # create table in postgres if it does not exist
    with open('sql/create_postgres_table.sql', 'r') as f:
        cursor.execute(f.read())
    conn.commit()

    # load our sql insert command
    with open('sql/insert_value.sql') as sql:
        insert = sql.read()

    client = storage.Client()
    bucket = client.get_bucket('airy-media-254122.appspot.com')
    blob = bucket.get_blob('bq_bucket/bq_dataset.csv')
    csv = blob.download_as_string().decode('utf-8').split('\n')
    rows = [row.split(',') for row in csv]
    for row in rows:
        if len(row) == 1:
            break
        row[6] = "NULL" if row[6] == 'None' else ("'"+str(datetime.utcfromtimestamp(float(row[6])))+"'")
        query = insert.format(
            row[0],row[1],row[2],row[3],row[4],
            str(datetime.utcfromtimestamp(float(row[5]))),
            row[6]
        )
        cursor.execute(query)

    conn.commit()
    cursor.close()
    conn.close()

# collects values based off date range from bigquery and pushes to gcs as csv
def bq_to_gcs(**kwargs):
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
        print(query)

    cursor.execute(query)

    #write to bucket
    with BUCKET.open('bq_bucket/bq_dataset.csv', 'w') as f:
        while True:
            result = cursor.fetchone()
            if result is None:
                break
            f.write(','.join([str(val) for val in result]) + '\n')

    cursor.close()
    conn.close()

def gcs_client_test(**kwargs):
    conn = PostgresHook(postgres_conn_id='my_local_db').get_conn()
    cursor = conn.cursor()
    cursor.execute("select * from austin_service_reports where unique_key='14-00000421';")
    res = cursor.fetchone()
    print(res)
    res = cursor.fetchone()
    print(res)