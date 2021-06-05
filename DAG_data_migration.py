from airflow import DAG
from operators.postgres import DataTransferPostgres
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
    dag_id="DataMigration",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    max_active_runs=1,
    tags=['data-flow'],
) as dag1:
    # это как было - частный DAG
    # сам data flow, для таблицы part
    #data_transfer = DataTransferPostgres(
    #    config={'table': 'public.part'},
    #    query='select * from part',
    #    task_id='part',
    #    source_pg_conn_str="host='host.docker.internal' port=5433 dbname='my_database_1' user='root' password='postgres'",
    #    pg_conn_str="host='host.docker.internal' port=54320 dbname='my_database_2' user='root' password='postgres'",
    #)
    # Порядок выполнения тасок
    #data_transfer

    data_dump = {
        table: DataTransferPostgres(
            config={'table': f'public.{table}'},
            query=f'select * from {table}',
            task_id=f'{table}',
            source_pg_conn_str="host='host.docker.internal' port=5433 dbname='my_database_1' user='root' password='postgres'",
            target_pg_conn_str="host='host.docker.internal' port=54320 dbname='my_database_2' user='root' password='postgres'",
            pg_meta_conn_str="host='host.docker.internal' port=54320 dbname='my_database_2' user='root' password='postgres'"
        )
        for table in source_table_list()
    }
