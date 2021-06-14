CREATE SCHEMA IF NOT EXISTS dds;

drop table if exists dds.h_nation, dds.h_region, dds.l_nation_region, dds.s_nation, dds.s_region;


create table dds.h_nation (
    h_nation_rk SERIAL PRIMARY KEY,
    nation_bk int4,
    launch_id int,
	effective_dttm timestamp default now(),
    UNIQUE(nation_bk)
);


create table dds.h_region (
    h_region_rk SERIAL PRIMARY KEY,
    region_bk int4,
    launch_id int,
	effective_dttm timestamp default now(),
    UNIQUE(region_bk)
);


create table dds.l_nation_region (
    l_nation_region_rk SERIAL PRIMARY KEY,
	h_nation_rk int4,
    h_region_rk int4,
    launch_id int,
    effective_dttm timestamp default now()
);


create table dds.s_nation (
    h_nation_rk int4 NOT NULL,
    name CHAR(25) NOT NULL, 
	comment VARCHAR(152),
    launch_id int,
    effective_dttm timestamp default now()
);

create table dds.s_region (
    h_region_rk int4 NOT NULL,
	name CHAR(25) NOT NULL, 
	comment VARCHAR(152),
    launch_id int,
    effective_dttm timestamp default now()
);
