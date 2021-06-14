CREATE SCHEMA IF NOT EXISTS sal;

drop table if exists sal.customer, sal.lineitem, sal.nation, sal.orders, sal.part, sal.partsupp, sal.region, sal.supplier;

CREATE TABLE sal.NATION  (NATION_bk  INTEGER NOT NULL,
                      NAME         CHAR(25) NOT NULL,
                      REGION_bk    INTEGER NOT NULL,
                      COMMENT      VARCHAR(152),
					  launch_id    INTEGER NOT NULL DEFAULT -100,
					  effective_dttm timestamp default now());

CREATE TABLE sal.REGION  (REGION_bk  INTEGER NOT NULL,
                      NAME       CHAR(25) NOT NULL,
                      COMMENT    VARCHAR(152),
					  launch_id    INTEGER NOT NULL DEFAULT -100,
					  effective_dttm timestamp default now());

CREATE TABLE sal.PART  (PART_bk     INTEGER NOT NULL,
                    NAME         VARCHAR(55) NOT NULL,
                    MFGR         CHAR(25) NOT NULL,
                    BRAND        CHAR(10) NOT NULL,
                    TYPE         VARCHAR(25) NOT NULL,
                    SIZE         INTEGER NOT NULL,
                    CONTAINER    CHAR(10) NOT NULL,
                    RETAILPRICE  DECIMAL(15,2) NOT NULL,
                    COMMENT      VARCHAR(23) NOT NULL,
					launch_id    INTEGER NOT NULL DEFAULT -100,
					effective_dttm timestamp default now());

CREATE TABLE sal.SUPPLIER ( supplier_bk     INTEGER NOT NULL,
                        NAME         CHAR(25) NOT NULL,
                        ADDRESS      VARCHAR(40) NOT NULL,
                        NATION_bk   INTEGER NOT NULL,
                        PHONE        CHAR(15) NOT NULL,
                        ACCTBAL      DECIMAL(15,2) NOT NULL,
                        COMMENT      VARCHAR(101) NOT NULL,
						launch_id    INTEGER NOT NULL DEFAULT -100,
					    effective_dttm timestamp default now());

CREATE TABLE sal.PARTSUPP ( PART_bk     INTEGER NOT NULL,
                        SUPPlier_bk     INTEGER NOT NULL,
                        AVAILQTY    INTEGER NOT NULL,
                        SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
                        COMMENT     VARCHAR(199) NOT NULL,
						launch_id    INTEGER NOT NULL DEFAULT -100,
					    effective_dttm timestamp default now());

CREATE TABLE sal.CUSTOMER ( CUSTomer_bk     INTEGER NOT NULL,
                        NAME         VARCHAR(25) NOT NULL,
                        ADDRESS      VARCHAR(40) NOT NULL,
                        NATION_bk    INTEGER NOT NULL,
                        PHONE        CHAR(15) NOT NULL,
                        ACCTBAL      DECIMAL(15,2)   NOT NULL,
                        MKTSEGMENT   CHAR(10) NOT NULL,
                        COMMENT      VARCHAR(117) NOT NULL,
						launch_id    INTEGER NOT NULL DEFAULT -100,
					    effective_dttm timestamp default now());
						  

CREATE TABLE sal.ORDERS  ( orders_bk       INTEGER NOT NULL,
                        CUSTomer_bk        INTEGER NOT NULL,
                        ORDERSTATUS    CHAR(1) NOT NULL,
                        TOTALPRICE     DECIMAL(15,2) NOT NULL,
                        ORDERDATE      DATE NOT NULL,
                        ORDERPRIORITY  CHAR(15) NOT NULL,  
                        CLERK          CHAR(15) NOT NULL, 
                        SHIPPRIORITY   INTEGER NOT NULL,
                        COMMENT        VARCHAR(79) NOT NULL,
						launch_id    INTEGER NOT NULL DEFAULT -100,
					    effective_dttm timestamp default now());

CREATE TABLE sal.LINEITEM ( orders_bk    INTEGER NOT NULL,
                          PART_BK        INTEGER NOT NULL,
                          SUPPlier_bk    INTEGER NOT NULL,
                          LINENUMBER     INTEGER NOT NULL,
                          QUANTITY       DECIMAL(15,2) NOT NULL,
                          EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
                          DISCOUNT       DECIMAL(15,2) NOT NULL,
                          TAX            DECIMAL(15,2) NOT NULL,
                          RETURNFLAG     CHAR(1) NOT NULL,
                          LINESTATUS     CHAR(1) NOT NULL,
                          SHIPDATE       DATE NOT NULL,
                          COMMITDATE     DATE NOT NULL,
                          RECEIPTDATE    DATE NOT NULL,
                          SHIPINSTRUCT   CHAR(25) NOT NULL,
                          SHIPMODE       CHAR(10) NOT NULL,
                          COMMENT        VARCHAR(44) NOT NULL,
						  launch_id    INTEGER NOT NULL DEFAULT -100,
					      effective_dttm timestamp default now());