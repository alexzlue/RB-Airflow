from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime, timedelta

SQL_PATH = 'dags/transfer_data_pipeline/sql/'

def average_days_open():
    conn = PostgresHook(postgres_conn_id='my_local_db').get_conn()
    cursor = conn.cursor()
    cursor.execute('SELECT AVG(average_num_days_open) FROM airflow.metrics_by_day')
    days_open = cursor.fetchone()

    cursor.close()
    conn.close()
    return int(days_open[0])


def load_transfer(**kwargs):
    # Collect days using function that accesses postgres
    days_open = average_days_open()

    conn = PostgresHook(postgres_conn_id='my_local_db').get_conn()
    cursor = conn.cursor()

    # Find most recent pull 
    cursor.execute('SELECT MAX(last_update_date) FROM airflow.transfer_table')
    start_date = cursor.fetchone()[0]
    if not start_date:
        cursor.execute('SELECT MIN(last_update_date) FROM airflow.austin_service_reports')
        start_date = cursor.fetchone()[0]+timedelta(seconds=1)
    
    end_date = datetime.strptime(kwargs['ds'], '%Y-%m-%d').date()-timedelta(days=days_open)

    # Remove previous data in transfer table
    cursor.execute('DELETE FROM airflow.transfer_table')

    with open(SQL_PATH + 'insert_into_transfer_table.sql') as f:
        insert = f.read().format(start_date, end_date)

    cursor.execute(insert)
    conn.commit()

    cursor.close()
    conn.close()
