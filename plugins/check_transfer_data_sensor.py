from datetime import datetime, timedelta
from random import randint

from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

class CheckTransferDataSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self, pg_conn_id, bq_conn_id, *args, **kwargs):
        self.pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)
        self.bq_hook = BigQueryHook(bigquery_conn_id=bq_conn_id, use_legacy_sql=False)
        super(CheckTransferDataSensor, self).__init__(*args, **kwargs)
    
    def poke(self, context):
        min_date = self.pg_hook.get_records('SELECT MIN(last_update_date) FROM airflow.transfer_table;')[0][0]
        max_date = self.pg_hook.get_records('SELECT MAX(last_update_date) FROM airflow.transfer_table;')[0][0]

        bq_query='''#standardSQL
        SELECT unique_key 
        FROM `bigquery-public-data.austin_311.311_service_requests` 
        WHERE last_update_date>='{0}' AND last_update_date<='{1}';'''
        pg_query='''SELECT unique_key 
        FROM airflow.transfer_table'''

        pg_results=self.pg_hook.get_records(pg_query)

        key_set = set([item[0] for item in pg_results])
        for record in self.bq_hook.get_records(bq_query.format(min_date, max_date)):
            if record[0] not in key_set:
                return False
        return True
        

class CheckDBPlugin(AirflowPlugin):
    name='check_transfer_data_sensor'
    sensors=[CheckTransferDataSensor]
