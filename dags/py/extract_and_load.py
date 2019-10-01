from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.hooks.postgres_hook import PostgresHook

from google.cloud import storage
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
    blob = bucket.get_blob('bq_bucket/austin_311_service_requests.csv')
    csv = blob.download_as_string().decode('utf-8').split('\n')
    rows = [row.split(',') for row in csv]
    insert = "INSERT INTO austin_service_reports VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}');"
    i=0
    for row in rows:
        if i!=0:
            if i>10:
                break
            query = insert.format(str(row[0]),str(row[2]),str(row[3]),str(row[4]),str(row[5]),str(row[7]),str(row[9]))
            cursor.execute(query)
        i+=1
    
    conn.commit()
    conn.close()
    cursor.close()

# decides to either load or create table
def branch_task(**kwargs):
    conn = PostgresHook(postgres_conn_id='my_local_db').get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT 1 FROM austin_service_reports;')
        print('noooo')
        return "load_table_task"
    except:
        print("yoooo")
        return 'create_table_task'