from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime, timedelta

SQL_PATH = 'dags/transfer_data_pipeline/sql/'

def average_days_open(**kwargs):
    conn = PostgresHook(postgres_conn_id='my_local_db').get_conn()
    cursor = conn.cursor()
    cursor.execute('SELECT AVG(average_num_days_open) FROM airflow.metrics_by_day')
    resp = cursor.fetchone()

    cursor.close()
    conn.close()
    return {'days_open':int(resp[0])}


def load_transfer(**kwargs):
    # Collect days from previous operator with xcom
    days_open = kwargs['task_instance'].xcom_pull(task_ids='task_calculate_avg_days')['days_open']
    
    conn = PostgresHook(postgres_conn_id='my_local_db').get_conn()
    cursor = conn.cursor()

    # Find most recent pull 
    cursor.execute('SELECT MAX(last_update_date) FROM airflow.transfer_table')
    start_date = cursor.fetchone()[0]
    if not start_date:
        cursor.execute('SELECT MIN(last_update_date) FROM airflow.austin_service_reports')
        start_date = cursor.fetchone()[0]+timedelta(seconds=1)
    
    # Remove previous data in transfer table
    cursor.execute('DELETE FROM airflow.transfer_table')
    
    # Calculate end date
    end_date = datetime.strptime(kwargs['ds'], '%Y-%m-%d').date()-timedelta(days=days_open)

    with open(SQL_PATH + 'insert_into_transfer_table.sql') as f:
        insert = f.read()
    insert = insert.format(start_date, end_date)
    # print(insert)

    # Insert into the new table from raw data table (from BQ)
    cursor.execute(insert)
    conn.commit()

    cursor.close()
    conn.close()

def transfer_to_aggregate(**kwargs):
    with open(SQL_PATH + 'insert_into_aggregate.sql') as f:
        insert = f.read()
    
    conn = PostgresHook(postgres_conn_id='my_local_db').get_conn()
    cursor = conn.cursor()

    cursor.execute(insert)

    conn.commit()
    cursor.close()
    conn.close()