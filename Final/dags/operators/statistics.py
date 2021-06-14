import logging
import os
import time
import re

import psycopg2
from airflow.utils.decorators import apply_defaults

from operators.data_transfer import DataTransfer


class WriteStatisticsOperator(DataTransfer):
    @apply_defaults
    def __init__(self, config, target_pg_conn_str, *args, **kwargs):
        super(DataTransfer, self).__init__(
            *args,
            **kwargs
        )
        self.config = config
        self.target_pg_conn_str = target_pg_conn_str

    def execute(self, context):

        schema_name = "{table}".format(**self.config).split(".")
        self.config.update(
            target_schema=schema_name[0],
            target_table=schema_name[1],
            job_id=context["task_instance"].job_id,  # modify
            dt=context["task_instance"].execution_date,  # modify
        )

        with psycopg2.connect(self.target_pg_conn_str) as conn, conn.cursor() as cursor:
            cursor.execute(
                """
            set search_path to {target_schema};
            select column_name
              from information_schema.columns
             where table_schema = '{target_schema}'
               and table_name = '{target_table}'
               and column_name not in ('launch_id', 'effective_dttm');
            """.format(
                    **self.config
                )
            )
            result = cursor.fetchall()
            columns = ", ".join('"{}"'.format(row) for row, in result)
            self.config.update(columns=columns)
            column_list = list(columns.split(', '))
            for item in column_list:

                self.config.update(column=item)
                with psycopg2.connect(self.target_pg_conn_str) as conn, conn.cursor() as cursor:
                    cursor.execute(
                        """
                        set search_path to {target_schema};
                        select count({column}) from {target_table} where {column} is not null
                        """.format(
                            **self.config)
                    )
                    result = cursor.fetchone()
                    result = re.search(r'\d+', str(result))
                    self.config.update(cnt_all=result[0])
                with psycopg2.connect(self.target_pg_conn_str) as conn, conn.cursor() as cursor:
                    cursor.execute(
                        """ 
                        set search_path to {target_schema};
                        select count({column}) from {target_table} where {column} is null;
                        """.format(
                            **self.config)
                    )
                    result = cursor.fetchone()
                    result = re.search(r'\d+', str(result))
                    self.config.update(cnt_nulls=result[0])

                self.write_etl_statistic(self.config)
