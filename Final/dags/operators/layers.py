import datetime
import json
import logging
import time

import psycopg2
from airflow.utils.decorators import apply_defaults

from operators.utils import DataFlowBaseOperator


class SalOperator(DataFlowBaseOperator):  # sae -> sal
    defaults = {
        'target_schema': 'sal',
        'source_schema': 'sae',
    }

    @apply_defaults
    def __init__(self, config, target_pg_conn_str, query=None, *args, **kwargs):
        super(SalOperator, self).__init__(
            config=config,
            target_pg_conn_str=target_pg_conn_str,
            *args,
            **kwargs
        )
        self.target_pg_conn_str = target_pg_conn_str
        self.config = dict(self.defaults, **config)
        self.query = query

    def execute(self, context):
        with psycopg2.connect(self.target_pg_conn_str) as conn, conn.cursor() as cursor:
            self.config.update(
                job_id=context['task_instance'].job_id,
                dt=context["task_instance"].execution_date,
            )
            ids = self.get_launch_ids(self.config)
            self.log.info("Ids found: {}".format(ids))

            for launch_id in ids:
                start = time.time()
                self.config.update(
                    launch_id=launch_id,
                )

                cols_sql = """
                select column_name
                     , data_type
                  from information_schema.columns
                 where table_schema = '{target_schema}'
                   and table_name = '{target_table}'
                   and column_name not in ('launch_id', 'effective_dttm');
                """.format(**self.config)

                cursor.execute(cols_sql)
                cols_list = list(cursor.fetchall())
                cols_dtypes = ",\n".join(
                    ('{}::{}'.format(col[0], col[1]) for col in cols_list))
                cols = ",\n".join(col[0] for col in cols_list)
                if self.query:
                    transfer_sql = """
                    with x as ({query})
                    insert into {target_schema}.{target_table} (launch_id, {cols})
                    select {job_id}::int as launch_id,\n{cols_dtypes}\n from x
                    """.format(query=self.query, cols_dtypes=cols_dtypes, cols=cols, **self.config)
                else:
                    transfer_sql = """
                    insert into {target_schema}.{target_table} (launch_id, {cols})
                    select {job_id}::int as launch_id,\n{cols_dtypes}\n from {source_schema}.{source_table}
                    """.format(cols_dtypes=cols_dtypes, cols=cols, **self.config)
                self.log.info('Executing query: {}'.format(transfer_sql))
                cursor.execute(transfer_sql)

                self.config.update(
                    source_schema='{source_schema}'.format(**self.config),
                    duration=datetime.timedelta(seconds=time.time() - start),
                    row_count=cursor.rowcount
                )
                self.log.info(
                    'Inserted rows: {row_count}'.format(**self.config))
                self.write_etl_log(self.config)


class DdsHOperator(DataFlowBaseOperator):  # sal -> dds for hubs
    defaults = {
        'target_schema': 'dds',
        'source_schema': 'sal',
    }

    @apply_defaults
    def __init__(self, config, target_pg_conn_str, *args, **kwargs):
        self.config = dict(
            self.defaults,
            target_table='h_{hub_name}'.format(**config),
            hub_bk='{hub_name}_bk'.format(**config),
            **config
        )
        super(DdsHOperator, self).__init__(
            config=config,
            target_pg_conn_str=target_pg_conn_str,
            *args,
            **kwargs
        )
        self.target_pg_conn_str = target_pg_conn_str

    def execute(self, context):
        with psycopg2.connect(self.target_pg_conn_str) as conn, conn.cursor() as cursor:

            self.config.update(
                job_id=context['task_instance'].job_id,
                dt=context["task_instance"].execution_date,
            )
            ids = self.get_launch_ids(self.config)
            self.log.info("Ids found: {}".format(ids))

            for launch_id in ids:
                start = time.time()

                self.config.update(
                    launch_id=launch_id
                )

                insert_sql = '''
                with x as (
                    select {bk_column}, {job_id} from {source_schema}.{source_table}
                    where {bk_column} is not null and launch_id = {launch_id}
                    group by 1
                )
                insert into {target_schema}.{target_table} ({hub_bk}, launch_id)
                select * from x;
                '''.format(**self.config)

                self.log.info('Executing query: {}'.format(insert_sql))
                cursor.execute(insert_sql)

                self.config.update(
                    row_count=cursor.rowcount
                )
                self.log.info('Row count: {row_count}'.format(**self.config))
                self.config.update(
                    duration=datetime.timedelta(seconds=time.time() - start)
                )
                self.write_etl_log(self.config)


class DdsLOperator(DataFlowBaseOperator):  # sal -> dds for links
    defaults = {
        'target_schema': 'dds',
        'source_schema': 'sal',
    }

    @apply_defaults
    def __init__(self, config, target_pg_conn_str, *args, **kwargs):
        super(DdsLOperator, self).__init__(
            config=config,
            target_pg_conn_str=target_pg_conn_str,
            *args,
            **kwargs
        )
        self.config = dict(
            self.defaults,
            target_table='l_{link_name}'.format(**config),
            **config
        )
        self.target_pg_conn_str = target_pg_conn_str

    def execute(self, context):
        with psycopg2.connect(self.target_pg_conn_str) as conn, conn.cursor() as cursor:
            self.config.update(
                job_id=context['task_instance'].job_id,
                dt=context["task_instance"].execution_date,
            )
            ids = self.get_launch_ids(self.config)
            self.log.info("Ids found: {}".format(ids))
            hubs = ('{hubs}'.format(**self.config))
            hubs = hubs.replace("'", "").strip('[]').split(', ')
            for launch_id in ids:
                start = time.time()
                self.config.update(
                    launch_id=launch_id,
                )

                the_columns_list = []

                a = 'select distinct * from {source_schema}.{source_table} s \n'.format(
                    **self.config)
                for hub_name in hubs:
                    b = 'JOIN {target_schema}.h_{hub_name} \nON {target_schema}.h_{hub_name}.{hub_name}_bk = s.{hub_name}_bk \n'.format(
                        **self.config, hub_name=hub_name)
                    the_columns_list.append(f'h_{hub_name}_rk')
                    a = a + b
                a = a + 'WHERE s.launch_id = {launch_id}'.format(**self.config)
                str_column_list = str(the_columns_list).strip(
                    "[]").replace("'", "")
                c = f'WITH x AS ({a})\n'
                d = 'INSERT INTO {target_schema}.l_{link_name} ({str_column_list}, launch_id)\nSELECT {str_column_list}, {job_id} FROM x\n'.format(
                    **self.config, str_column_list=str_column_list)
                insert_query = c + d

                # insert_sql = '''
                # with x as (
                #     select distinct
                #            {l_hub_name}_id
                #          , {r_hub_name}_id
                #       from {source_schema}.{source_table} s
                #       join dds.h_{l_hub_name} l
                #       on s.{l_bk_column} = l.{l_hub_name}_id
                #       join dds.h_{r_hub_name} r
                #       on s.h_{r_bk_column} = r.{r_hub_name}_id
                #       where s.launch_id = {launch_id}
                # )
                # insert into {target_schema}.{target_table} ({l_hub_name}_id, {l_hub_name}_id, launch_id)
                # select {l_hub_name}_id
                #      , {r_hub_name}_id
                #      , {job_id}
                #   from x;
                # '''.format(**self.config)

                self.log.info('Executing query: {}'.format(insert_query))
                cursor.execute(insert_query)
                self.config.update(
                    row_count=cursor.rowcount,
                    duration=datetime.timedelta(seconds=time.time() - start)
                )
                self.log.info('Row count: {row_count}'.format(**self.config))
                self.write_etl_log(self.config)


class DdsHSOperator(DataFlowBaseOperator):  # sal -> dds for hub_satellites
    defaults = {
        'target_schema': 'dds',
        'source_schema': 'sal',
    }

    @apply_defaults
    def __init__(self, config, target_pg_conn_str, *args, **kwargs):
        super(DdsHSOperator, self).__init__(
            config=config,
            target_pg_conn_str=target_pg_conn_str,
            *args,
            **kwargs
        )
        self.config = dict(
            self.defaults,
            target_table='s_{hub_name}'.format(**config),
            **config
        )
        self.target_pg_conn_str = target_pg_conn_str

    def execute(self, context):
        with psycopg2.connect(self.target_pg_conn_str) as conn, conn.cursor() as cursor:
            self.config.update(
                job_id=context['task_instance'].job_id,
                dt=context["task_instance"].execution_date,
            )
            ids = self.get_launch_ids(self.config)
            self.log.info("Ids found: {}".format(ids))

            columns_info = '{columns_info}'.format(**self.config)
            columns_info = json.loads(columns_info.replace("'", '"'))
            self.log.info(f'columns info: {columns_info}')
            bk_list = []
            column_list = []
            self.log.info(f'columns list: {column_list}')
            for item in columns_info:
                for column_name, column_data in dict(item).items():
                    if column_data.get('bk_for') == '{hub_name}'.format(**self.config):
                        bk_list.append(column_name)
                    elif column_data.get('source_for') == '{hub_name}'.format(**self.config):
                        column_list.append(column_name)

            str_column_list = str(column_list).strip(
                "[]").replace("'", "")

            for launch_id in ids:
                start = time.time()
                self.config.update(
                    launch_id=launch_id,
                )

                insert_query = '''
                with x as (
                select distinct * from {source_schema}.{source_table} s 
                join {target_schema}.h_{hub_name} 
                on {target_schema}.h_{hub_name}.{hub_name}_bk = s.{hub_name}_bk
                where s.launch_id = {launch_id}
                )
                insert into {target_schema}.{target_table} (h_{hub_name}_rk, {str_column_list}, launch_id)
                select h_{hub_name}_rk, {str_column_list}, {job_id} from x;
                '''.format(**self.config, str_column_list=str_column_list)

                self.log.info('Executing query: {}'.format(insert_query))
                cursor.execute(insert_query)
                self.config.update(
                    row_count=cursor.rowcount,
                    duration=datetime.timedelta(seconds=time.time() - start)
                )
                self.log.info('Row count: {row_count}'.format(**self.config))
                self.write_etl_log(self.config)


class DdsLSOperator(DataFlowBaseOperator):  # sal -> dds for link_satellites
    defaults = {
        'target_schema': 'dds',
        'source_schema': 'sal',
    }

    @apply_defaults
    def __init__(self, config, target_pg_conn_str, *args, **kwargs):
        super(DdsLSOperator, self).__init__(
            config=config,
            target_pg_conn_str=target_pg_conn_str,
            *args,
            **kwargs
        )
        self.config = dict(
            self.defaults,
            target_table='s_l_{link_name}'.format(**config),
            **config
        )
        self.target_pg_conn_str = target_pg_conn_str

    def execute(self, context):
        with psycopg2.connect(self.target_pg_conn_str) as conn, conn.cursor() as cursor:
            self.config.update(
                job_id=context['task_instance'].job_id,
                dt=context["task_instance"].execution_date,
            )
            ids = self.get_launch_ids(self.config)
            self.log.info("Ids found: {}".format(ids))

            columns_info = '{columns_info}'.format(**self.config)
            columns_info = json.loads(columns_info.replace("'", '"'))
            self.log.info(f'columns info: {columns_info}')
            hubs = '{link_name}'.format(**self.config).split("_")
            bk_list = []
            column_list = []

            self.log.info('link name: {link_name}'.format(**self.config))
            for hub_name in hubs:
                self.log.info(f'hub name: {hub_name}')
                for item in columns_info:
                    self.log.info(f'item name: {item}')
                    for column_name, column_data in item.items():
                        self.log.info(f'column name: {column_name}')
                        self.log.info(f'column_data: {column_data}')
                        if column_data.get('bk_for') == f'{hub_name}':
                            bk_list.append(column_name)
                        elif column_data.get('source_for') == '{link_name}'.format(**self.config):
                            column_list.append(column_name)

            self.log.info(f'columns list: {column_list}')

            for launch_id in ids:
                start = time.time()
                self.config.update(
                    launch_id=launch_id,
                )
                bk_list = set(bk_list)
                column_list = set(column_list)
                str_column_list = str(column_list).strip(
                    "{}").replace("'", "")

                insert_query = 'select distinct * from {source_schema}.{source_table} s \n'.format(
                    **self.config)
                for hub_name in hubs:
                    hub_join = 'JOIN {target_schema}.h_{hub_name} \nON {target_schema}.h_{hub_name}.{hub_name}_bk = s.{hub_name}_bk \n'.format(
                        **self.config, hub_name=hub_name)
                    insert_query = insert_query + hub_join
                insert_query = insert_query + \
                    'JOIN {target_schema}.l_{link_name}\nON '.format(
                        **self.config)
                for hub_name in hubs:
                    if hub_name != hubs[-1]:
                        link_join = '{target_schema}.l_{link_name}.h_{hub_name}_rk = {target_schema}.h_{hub_name}.h_{hub_name}_rk AND '.format(
                            **self.config, hub_name=hub_name)
                        insert_query = insert_query + link_join
                    else:
                        link_join = '{target_schema}.l_{link_name}.h_{hub_name}_rk = {target_schema}.h_{hub_name}.h_{hub_name}_rk\n'.format(
                            **self.config, hub_name=hub_name)
                        insert_query = insert_query + link_join
                insert_query = insert_query + \
                    'WHERE s.launch_id = {launch_id}\n'.format(**self.config)
                insert_query = f'WITH x AS (\n{insert_query})\n'
                insert_query = insert_query + 'INSERT INTO {target_schema}.l_s_{link_name} (l_{link_name}_rk, {str_column_list}, launch_id)\nSELECT l_{link_name}_rk, {str_column_list}, {job_id} FROM x\n'.format(
                    **self.config, str_column_list=str_column_list)

                self.log.info('Executing query: {}'.format(insert_query))
                cursor.execute(insert_query)
                self.config.update(
                    row_count=cursor.rowcount,
                    duration=datetime.timedelta(seconds=time.time() - start)
                )
                self.log.info('Row count: {row_count}'.format(**self.config))
                self.write_etl_log(self.config)
