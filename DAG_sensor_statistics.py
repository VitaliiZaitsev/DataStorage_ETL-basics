from airflow import DAG
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from operators.statistics import WriteStatisticsOperator
from operators.utils import source_table_list
from datetime import datetime

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2021, 1, 25),
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False, # в случае регулярной эксплуатации следует поставить True. Сейчас указал False для целей отладки
}

with DAG(
    dag_id='SensorStatistics',
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
    tags=['data-flow-statistics'],
) as dag1:
    # sensor = ExternalTaskSensor(
    #     task_id='customer_sensor',
    #     external_dag_id='DataMigration',
    #     external_task_id='customer'
    # )
    # statistics = WriteStatisticsOperator(
    #     task_id='customer_statistics',
    #     config={'table': 'public.customer'},
    #     pg_meta_conn_str="host='host.docker.internal' port=54320 dbname='my_database_2' user='root' password='postgres'",
    #     target_pg_conn_str="host='host.docker.internal' port=54320 dbname='my_database_2' user='root' password='postgres'"
    # )

    for table in source_table_list():
        sensor = ExternalTaskSensor(
            task_id=f'{table}_sensor',
            external_dag_id='DataMigration',
            external_task_id=f'{table}'
        )

        statistics = WriteStatisticsOperator(
            config={'table': f'public.{table}'},
            task_id=f'{table}_statistics',
            pg_meta_conn_str="host='host.docker.internal' port=54320 dbname='my_database_2' user='root' password='postgres'",
            target_pg_conn_str="host='host.docker.internal' port=54320 dbname='my_database_2' user='root' password='postgres'"
        )

        sensor >> statistics
