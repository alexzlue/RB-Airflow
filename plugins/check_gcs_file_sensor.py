from datetime import datetime, timedelta
import os
import gcs_client

from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'keys/airy-media-254122-973505938453.json'
CRED = gcs_client.Credentials(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
PROJECT = gcs_client.Project('airy-media-254122', CRED)
BUCKET = PROJECT.list()[0]

class CheckGCSFileSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(CheckGCSFileSensor, self).__init__(*args, **kwargs)
    
    def poke(self, context):
        for bucket_object in BUCKET.list():
            if bucket_object.name=='bq_bucket/bq_dataset.txt':
                return True
        return False
        

class CheckGCSPlugin(AirflowPlugin):
    name='check_gcs_file_sensor'
    sensors=[CheckGCSFileSensor]
