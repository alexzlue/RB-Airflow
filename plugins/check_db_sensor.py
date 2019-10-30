from datetime import datetime, timedelta
from random import randint

from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

class CheckDBSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self, pg_conn_id, bq_conn_id, *args, **kwargs):
        self.pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)
        self.bq_hook = BigQueryHook(bigquery_conn_id=bq_conn_id, use_legacy_sql=False)
        super(CheckDBSensor, self).__init__(*args, **kwargs)
    
    def poke(self, context):
        ds = datetime.strptime(context['ds'],'%Y-%m-%d')

        bq_query='''#standardSQL
        SELECT unique_key 
        FROM `bigquery-public-data.austin_311.311_service_requests` 
        WHERE last_update_date>='{0}' AND last_update_date<'{1}';'''
        pg_query='''SELECT unique_key 
        FROM airflow.austin_service_reports 
        WHERE last_update_date>='{0}' AND last_update_date<'{1}';'''

        pg_results=self.pg_hook.get_records(pg_query.format(ds-timedelta(days=1), ds))

        key_set = set([item[0] for item in pg_results])
        for record in self.bq_hook.get_records(bq_query.format(ds-timedelta(days=1), ds)):
            if record[0] not in key_set:
                return False
        return True
        

class CheckDBPlugin(AirflowPlugin):
    name='check_postgres_db_sensor'
    sensors=[CheckDBSensor]
