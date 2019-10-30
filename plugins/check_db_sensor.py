from datetime import datetime
from random import randint

from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

class CheckDBSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self, pg_conn_id, bq_conn_id, start_date, end_date, *args, **kwargs):
        self.pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)
        self.bq_hook = BigQueryHook(bigquery_conn_id=bq_conn_id, use_legacy_sql=False)
        self.start_date = start_date
        self.end_date = end_date
        super(CheckDBSensor, self).__init__(*args, **kwargs)
    
    def poke(self, context):
        return True
        

class CheckDBPlugin(AirflowPlugin):
    name='check_postgres_db_sensor'
    sensors=[CheckDBSensor]
