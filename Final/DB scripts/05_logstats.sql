CREATE SCHEMA IF NOT EXISTS logstats;

drop table if exists logstats."log", logstats."statistic";

create table logstats."log" (
       source_launch_id    int
     , target_schema       text
     , target_table        text
     , target_launch_id    int
     , processed_dttm      timestamp default now()
     , row_count           int
     , duration            interval
     , load_date           date
);

create table logstats."statistic" (
       table_name     text
     , column_name    text
     , cnt_nulls      int
     , cnt_all        int
     , load_date      date
);