from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.hooks.postgres_hook import PostgresHook

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
    cursor.execute('SELECT * FROM austin_service_reports;')
    print(cursor.fetchall())
    
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