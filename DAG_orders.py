from airflow.models import DAG
from operators.postgres import DataTransferPostgres
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
    dag_id="DataFlow-orders",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    max_active_runs=1,
    tags=['data-flow'],
) as dag1:
    # сам data flow, для таблицы orders
    data_transfer = DataTransferPostgres(
        config={'table': 'public.orders'},
        query='select * from orders',
        task_id='orders',
        source_pg_conn_str="host='host.docker.internal' port=5433 dbname='my_database_1' user='root' password='postgres'",
        pg_conn_str="host='host.docker.internal' port=54320 dbname='my_database_2' user='root' password='postgres'",
    )
    
    # Порядок выполнения тасок
    data_transfer

