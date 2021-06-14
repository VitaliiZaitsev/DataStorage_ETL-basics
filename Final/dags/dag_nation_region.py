import os
from datetime import datetime
from pprint import pprint

import yaml
from airflow import DAG
from airflow.utils.dates import days_ago

from operators.layers import (DdsHOperator, DdsHSOperator, DdsLOperator,
                              DdsLSOperator, SalOperator)
from operators.postgres import DataTransferPostgres

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

#SAE_QUERY = 'select * from {table}'
SAE_QUERY = 'select * from src.{table}'


# ВНИМАНИЕ
# 1. Docker под Windows, иногда host='db1' НЕ работает, но работает host.docker.internal or postgres instead of localhost
# https://stackoverflow.com/questions/58272934/cannot-connect-to-docker-postgres-server-through-airflow-dag
# https://docs.docker.com/docker-for-windows/networking/
# 2. Очень аккуратно: здесь используем ВНУТРЕННИЕ порты 5432 (НЕ ВНЕШНИЕ)
# 3. Также аккуратно: dbname='my_database_1' user='root' password='postgres'
#SOURCE_CONNECTIONSTRING="host='host.docker.internal' port=5432 dbname='my_database_1' user='root' password='postgres'"
#TARGET_CONNECTIONSTRING="host='host.docker.internal' port=5432 dbname='my_database_2' user='root' password='postgres'"
#LOGSTATS_CONNECTIONSTRING="host='host.docker.internal' port=5432 dbname='my_database_2' user='root' password='postgres'"

SOURCE_CONNECTIONSTRING="host='db1' port=5432 dbname='my_database_1' user='root' password='postgres'"
TARGET_CONNECTIONSTRING="host='db2' port=5432 dbname='my_database_2' user='root' password='postgres'"
LOGSTATS_CONNECTIONSTRING="host='db2' port=5432 dbname='my_database_2' user='root' password='postgres'"


with DAG(
    dag_id="ETL-nation-region",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    max_active_runs=1,
    tags=['data-flow'],
) as dag1:
    sae = {
        table: DataTransferPostgres(
            config=dict(
                table='sae.{table}'.format(table=table)
            ),
            query=SAE_QUERY.format(table=table),
            task_id='sae_{table}'.format(table=table),
            source_pg_conn_str=SOURCE_CONNECTIONSTRING,
            target_pg_conn_str=TARGET_CONNECTIONSTRING,
            pg_meta_conn_str=LOGSTATS_CONNECTIONSTRING
        )
        for table in YAML_DATA['sources']['tables'].keys()
    }

    sal = {
        table: SalOperator(
            config=dict(
                target_table=table,
                source_table=table,
            ),
            task_id='sal_{table}'.format(table=table),
            pg_meta_conn_str=LOGSTATS_CONNECTIONSTRING,
            target_pg_conn_str=TARGET_CONNECTIONSTRING
        )
        for table in YAML_DATA['sources']['tables'].keys()
    }

    for target_table, task in sal.items():
        sae[target_table] >> task

    hubs = {
        hub_name: {
            table: DdsHOperator(
                task_id='dds.h_{hub_name}'.format(hub_name=hub_name),
                config={
                    'hub_name': hub_name,
                    'source_table': table,
                    'bk_column': bk_column
                },
                pg_meta_conn_str=LOGSTATS_CONNECTIONSTRING,
                target_pg_conn_str=TARGET_CONNECTIONSTRING
            )
            for table, cols in YAML_DATA['sources']['tables'].items()
            for col in cols['columns']
            for bk_column, inf in col.items()
            if inf.get('bk_for') == hub_name
        }
        for hub_name in YAML_DATA['groups']['hubs'].keys()
    }

    for hub, info in hubs.items():
        for source_table, task in info.items():
            sal[source_table] >> task

    links = {
        tuple(hubs_data): {
            hub: {
                table: DdsLOperator(
                    task_id='dds.l_{link_name}'.format(link_name=link_name),
                    pg_meta_conn_str=LOGSTATS_CONNECTIONSTRING,
                    target_pg_conn_str=TARGET_CONNECTIONSTRING,
                    config={
                        'source_table': source_table,
                        'link_name': link_name,
                        'hubs': hubs_data
                    }
                )
                for table, cols_info in YAML_DATA['sources']['tables'].items()
                for col in cols_info['columns']
                for bk_column, inf in col.items()
                if inf.get('bk_for') == hub

            }
            for hub in hubs_data

        }
        for link_name, link_info in YAML_DATA['groups']['links'].items()
        for source_table, hubs_data in link_info.items()
    }

    for link_info in links.values():
        for hub_name, info in link_info.items():
            for source_table, task in info.items():
                hubs[hub_name][source_table] >> task

    hub_satellites = {
        (hub_name, source_table): {
            table_name: DdsHSOperator(
                task_id='dds.s_{hub_name}'.format(hub_name=hub_name),
                pg_meta_conn_str=LOGSTATS_CONNECTIONSTRING,
                target_pg_conn_str=TARGET_CONNECTIONSTRING,
                config={
                    'hub_name': hub_name,
                    'columns_info': YAML_DATA['sources']['tables'][hub_name]['columns'],
                    'source_table': source_table
                }
            )
            for table_name, table_info in YAML_DATA['sources']['tables'].items()
            for column_info in table_info['columns']
            for info in column_info.values()
            if info.get('bk_for') == hub_name
        }
        for hub_name in YAML_DATA['groups']['hubs'].keys()
        for sat, source_table in YAML_DATA['groups']['satellites'].items()
        if hub_name == sat
    }

    for (hub_name, redundant), info in hub_satellites.items():
        for source_table, task in info.items():
            for source_table, task in info.items():
                hubs[hub_name][source_table] >> task

    link_satellites = {
        (tuple(link_name.split('_')), source_table): {
            hub: {
                table_name: DdsLSOperator(
                    task_id='dds.s_l_{link_name}'.format(link_name=link_name),
                    pg_meta_conn_str=LOGSTATS_CONNECTIONSTRING,
                    target_pg_conn_str=TARGET_CONNECTIONSTRING,
                    config={
                        'link_name': link_name,
                        'columns_info': YAML_DATA['sources']['tables'][source_table]['columns'],
                        'source_table': source_table
                    }
                )
                for table_name, table_info in YAML_DATA['sources']['tables'].items()
                for column_info in table_info['columns']
                for info in column_info.values()
                if info.get('bk_for') == hub

            }
            for hub in tuple(link_name.split('_'))

        }
        for link_name in YAML_DATA['groups']['links'].keys()
        for sat, source_table in YAML_DATA['groups']['satellites'].items()
        if link_name == sat
    }

    for (link_name, source_table), link_hub_info in link_satellites.items():
        for link_hub, table_info in link_hub_info.items():
            for table_name, task in table_info.items():
                links[link_name][link_hub][table_name] >> task
