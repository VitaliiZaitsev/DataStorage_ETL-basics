from airflow import DAG
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.dates import days_ago

from operators.statistics import WriteStatisticsOperator
#from operators.utils import source_table_list

import yaml
import os

with open(os.path.join(os.path.dirname(__file__), 'schema_nation_region.yaml'), encoding='utf-8') as f:
    YAML_DATA = yaml.safe_load(f)
    
    
DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
}

SOURCE_CONNECTIONSTRING="host='db1' port=5432 dbname='my_database_1' user='root' password='postgres'"
TARGET_CONNECTIONSTRING="host='db2' port=5432 dbname='my_database_2' user='root' password='postgres'"
LOGSTATS_CONNECTIONSTRING="host='db2' port=5432 dbname='my_database_2' user='root' password='postgres'"


with DAG(
    dag_id='ETL-nation-region-data-statistics',
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily',
    catchup=True,
    max_active_runs=1,
    tags=['data-flow-statistics'],
) as dag1:

    for table in ['nation', 'region']:
        sensor = ExternalTaskSensor(
            task_id=f'sae_{table}',
            external_dag_id='ETL-nation-region',
            external_task_id=f'sae_{table}'
        )

        statistics = WriteStatisticsOperator(
            config={'table': f'sae.{table}'},
            task_id=f'{table}_statistics',
            pg_meta_conn_str=LOGSTATS_CONNECTIONSTRING,
            target_pg_conn_str=TARGET_CONNECTIONSTRING
        ) 
        
        sensor >> statistics
