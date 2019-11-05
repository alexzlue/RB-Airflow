from datetime import datetime, timedelta
import os
import gcs_client

from airflow.hooks.postgres_hook import PostgresHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

class CheckBacklogSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self, pg_conn_id, *args, **kwargs):
        self.pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)
        super(CheckBacklogSensor, self).__init__(*args, **kwargs)
    
    def poke(self, context):
        # Query to check whether austin service reports has the
        # most updated entries, and backlog has old entries
        backlog_query = '''
        -- collects most recent backlog timestamp
        WITH max_backlog AS (
            SELECT MAX(backlog_ts) AS max_ts
            FROM airflow.austin_service_reports_backlog
        ),
        -- collects all backed up entries from backlog timestamp
        recent_backlog AS(
            SELECT id, unique_key, last_update_date 
            FROM airflow.austin_service_reports_backlog, max_backlog AS mb
            WHERE mb.max_ts=backlog_ts
        )
        -- collects all keys from the backlog that had last_update_date greater 
        -- than the value in austin_service_reports table
        SELECT t2.unique_key
        FROM airflow.austin_service_reports AS t1, recent_backlog AS t2
        WHERE t1.unique_key=t2.unique_key AND t2.last_update_date > t1.last_update_date;
        '''
        results = self.pg_hook.get_records(backlog_query)

        # there should not be anything in the backlog that is
        # more recent than in the austin_service_reports table
        # so results should be empty, or length 0.
        return not bool(len(results))
        

class CheckBacklogPlugin(AirflowPlugin):
    name='check_backlog_sensor'
    sensors=[CheckBacklogSensor]
