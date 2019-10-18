from airflow.hooks.postgres_hook import PostgresHook

SQL_PATH = 'dags/transfer_data_pipeline/sql/'

def average_days_open(**kwargs):
    conn = PostgresHook(postgres_conn_id='my_local_db').get_conn()
    cursor = conn.cursor()
    cursor.execute('SELECT AVG(average_num_days_open) FROM airflow.metrics_by_day')
    resp = cursor.fetchone()

    cursor.close()
    conn.close()
    return int(resp[0])


def create_temp_table(**kwargs):
    day_diff = kwargs['task_instance'].xcom_pull(task_ids='task_calculate_avg_days')
    # with open(SQL_PATH + '')
    print(day_diff)

