from airflow.contrib.hooks.bigquery_hook import BigQueryHook


def bq_pull(**kwargs):
    cursor = BigQueryHook(
        bigquery_conn_id='my_gcp_connection',
        use_legacy_sql=False).get_conn().cursor()
    with open('sql/load_bq_dataset.sql', 'r') as f:
        cursor.execute(f.read())
    result = cursor.fetchmany(5)
    print(result)
    return result