-- Task 1

USE warehouse compute_wh;

CREATE DATABASE EPAM_LAB;


-- Task 2

CREATE SCHEMA CORE_DWH;
CREATE SCHEMA DATA_MART;

USE DATABASE epam_lab;
USE SCHEMA core_dwh;

CREATE OR REPLACE TABLE CORE_DWH.region
(
  r_regionkey INTEGER,
  r_name      CHAR(25),
  r_comment   VARCHAR(152)
);


CREATE OR REPLACE TABLE CORE_DWH.nation
(
  n_nationkey INTEGER not null,
  n_name      CHAR(27),
  n_regionkey INTEGER,
  n_comment   VARCHAR(155)
);


CREATE OR REPLACE TABLE CORE_DWH.supplier
(
  s_suppkey   INTEGER not null,
  s_name      CHAR(25),
  s_address   VARCHAR(40),
  s_nationkey INTEGER,
  s_phone     CHAR(15),
  s_acctbal   FLOAT8,
  s_comment   VARCHAR(101)
);


CREATE OR REPLACE TABLE CORE_DWH.part
(
  p_partkey     INTEGER not null,
  p_name        VARCHAR(55),
  p_mfgr        CHAR(25),
  p_brand       CHAR(10),
  p_type        VARCHAR(25),
  p_size        INTEGER,
  p_container   CHAR(10),
  p_retailprice INTEGER,
  p_comment     VARCHAR(23)
);


CREATE OR REPLACE TABLE CORE_DWH.partsupp
(
  ps_partkey    INTEGER not null,
  ps_suppkey    INTEGER not null,
  ps_availqty   INTEGER,
  ps_supplycost FLOAT8 not null,
  ps_comment    VARCHAR(199)
);


CREATE OR REPLACE TABLE CORE_DWH.customer
(
  c_custkey    INTEGER not null,
  c_name       VARCHAR(25),
  c_address    VARCHAR(40),
  c_nationkey  INTEGER,
  c_phone      CHAR(15),
  c_acctbal    FLOAT8,
  c_mktsegment CHAR(10),
  c_comment    VARCHAR(117)
);


CREATE OR REPLACE TABLE CORE_DWH.orders
(
  o_orderkey      INTEGER not null,
  o_custkey       INTEGER not null,
  o_orderstatus   CHAR(1),
  o_totalprice    FLOAT8,
  o_orderdate     DATE,
  o_orderpriority CHAR(15),
  o_clerk         CHAR(15),
  o_shippriority  INTEGER,
  o_comment       VARCHAR(79)
);


CREATE OR REPLACE TABLE CORE_DWH.lineitem
(
  l_orderkey      INTEGER not null,
  l_partkey       INTEGER not null,
  l_suppkey       INTEGER not null,
  l_linenumber    INTEGER not null,
  l_quantity      INTEGER not null,
  l_extendedprice FLOAT8 not null,
  l_discount      FLOAT8 not null,
  l_tax           FLOAT8 not null,
  l_returnflag    CHAR(1),
  l_linestatus    CHAR(1),
  l_shipdate      DATE,
  l_commitdate    DATE,
  l_receiptdate   DATE,
  l_shipinstruct  CHAR(25),
  l_shipmode      CHAR(10),
  l_comment       VARCHAR(44)
);


CREATE STAGE EPAM_LAB.CORE_DWH.epam_lab 
URL = 's3://epam-lab-vatsyk';

list @epam_lab;

CREATE OR REPLACE FILE FORMAT EPAM_LAB.CORE_DWH.CSV TYPE = 'CSV'
COMPRESSION = 'AUTO' 
FIELD_DELIMITER = ',' 
RECORD_DELIMITER = '\n' 
SKIP_HEADER = 1 
FIELD_OPTIONALLY_ENCLOSED_BY = '\042'
TRIM_SPACE = FALSE 
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE 
ESCAPE = 'NONE' 
ESCAPE_UNENCLOSED_FIELD = '\134' 
DATE_FORMAT = 'dd.mm.yy' 
TIMESTAMP_FORMAT = 'AUTO' 
NULL_IF = ('');


COPY INTO customer from @epam_lab
file_format=CSV
pattern='.*customer.*.csv';


COPY INTO region from @epam_lab
file_format=CSV
pattern='.*region.*.csv';


COPY INTO nation from @epam_lab
file_format=CSV
pattern='.*nation.*.csv';


COPY INTO part from @epam_lab
file_format=CSV
pattern='.*part.csv';


COPY INTO supplier from @epam_lab
file_format=CSV
pattern='.*supplier.*.csv';


COPY INTO partsupp from @epam_lab
file_format=CSV
pattern='.*partsupp.*.csv';


COPY INTO orders from @epam_lab
file_format=CSV
pattern='.*order.*.csv';


COPY INTO lineitem from @epam_lab
file_format=CSV
pattern='.*lineitem.*.csv';


-- Task 7

CREATE STAGE EPAM_LAB.CORE_DWH.snowpipe_orders 
URL = 's3://test-snowpipe-epam-lab/h_order';


CREATE STAGE EPAM_LAB.CORE_DWH.snowpipe_lineitems 
URL = 's3://test-snowpipe-epam-lab/h_lineitem';


CREATE OR REPLACE PIPE EPAM_LAB.CORE_DWH.orders_pipe auto_ingest = true as
COPY INTO EPAM_LAB.CORE_DWH.orders from @EPAM_LAB.CORE_DWH.snowpipe_orders 
file_format = EPAM_LAB.CORE_DWH.CSV;


CREATE OR REPLACE PIPE EPAM_LAB.CORE_DWH.lineitems_pipe auto_ingest = true as
COPY INTO EPAM_LAB.CORE_DWH.lineitem from @EPAM_LAB.CORE_DWH.snowpipe_lineitems 
file_format = EPAM_LAB.CORE_DWH.CSV;


show pipes;


-- Task 3


CREATE OR REPLACE STREAM CORE_DWH.customers_stream
ON TABLE CORE_DWH.customer
APPEND_ONLY = TRUE
SHOW_INITIAL_ROWS = TRUE;


CREATE OR REPLACE STREAM CORE_DWH.lineitems_stream
ON TABLE CORE_DWH.lineitem
APPEND_ONLY = TRUE
SHOW_INITIAL_ROWS = TRUE;


CREATE OR REPLACE STREAM CORE_DWH.orders_stream
ON TABLE CORE_DWH.orders
APPEND_ONLY = TRUE
SHOW_INITIAL_ROWS = TRUE;


CREATE OR REPLACE STREAM CORE_DWH.parts_stream
ON TABLE CORE_DWH.part
APPEND_ONLY = TRUE
SHOW_INITIAL_ROWS = TRUE;


CREATE OR REPLACE STREAM CORE_DWH.partsupp_stream
ON TABLE CORE_DWH.partsupp
APPEND_ONLY = TRUE
SHOW_INITIAL_ROWS = TRUE;


CREATE OR REPLACE STREAM CORE_DWH.suppliers_stream
ON TABLE CORE_DWH.supplier
APPEND_ONLY = TRUE
SHOW_INITIAL_ROWS = TRUE;


USE warehouse compute_wh;
USE DATABASE epam_lab;
USE SCHEMA data_mart;


CREATE OR REPLACE PROCEDURE dates_procedure(start_date date, end_date date)
returns string
language sql
as
$$
declare
num_days integer;
begin 
num_days := (select datediff(DAY, :start_date, :end_date));
CREATE OR REPLACE TABLE DATA_MART.d_dates 
(
  d_datekey      CHAR(8) not null,
  date_val       DATE not null,
  day_of_mon     SMALLINT not null,
  day_of_week    SMALLINT not null,
  day_name       CHAR(3) not null,
  month_num      SMALLINT not null,
  month_name     CHAR(3) not null,
  week_of_year   SMALLINT not null,
  quarter        SMALLINT not null,
  year           SMALLINT not null
)
AS
  WITH CTE_MY_DATE AS (
    SELECT DATEADD(DAY, SEQ4(), :start_date) AS date_val
      FROM TABLE(GENERATOR(ROWCOUNT=> :num_days))  
  )
  SELECT REPLACE(REPLACE(TO_CHAR(date_val), ' 00:00:00.000', ''), '-', ''),
         date_val,
         DAY(date_val),
         DAYOFWEEK(date_val),
         DAYNAME(date_val),
         MONTH(date_val),
         MONTHNAME(date_val),
         WEEKOFYEAR(date_val),
         QUARTER(date_val),
         YEAR(date_val)
    FROM CTE_MY_DATE;
    return 'Completed';
end;
$$
;

call dates_procedure('1990-01-01', '2023-01-01');


CREATE OR REPLACE TABLE DATA_MART.d_supplier
(
  s_suppkey   INTEGER not null,
  s_name      CHAR(25),
  s_address   VARCHAR(40),
  s_nation    CHAR(27),
  s_region    CHAR(25),
  s_phone     CHAR(15),
  s_acctbal   FLOAT8
);


CREATE OR REPLACE TABLE DATA_MART.d_part
(
  p_partkey     INTEGER not null,
  p_name        VARCHAR(55),
  p_mfgr        CHAR(25),
  p_brand       CHAR(10),
  p_type        VARCHAR(25),
  p_size        INTEGER,
  p_container   CHAR(10),
  p_retailprice INTEGER
);


CREATE OR REPLACE TABLE DATA_MART.d_customer
(
  c_custkey    INTEGER not null,
  c_name       VARCHAR(25),
  c_address    VARCHAR(40),
  c_nation     CHAR(27),
  c_phone      CHAR(15),
  c_acctbal    FLOAT8,
  c_mktsegment CHAR(10)
);


CREATE OR REPLACE TABLE DATA_MART.f_lineitem
(
  l_orderkey      INTEGER not null,
  l_partkey       INTEGER not null,
  l_suppkey       INTEGER not null,
  l_linenumber    INTEGER not null,
  l_custkey       INTEGER not null,
  l_quantity      INTEGER not null,
  l_extendedprice FLOAT8 not null,
  l_discount      FLOAT8 not null,
  l_tax           FLOAT8 not null,
  l_returnflag    CHAR(1),
  l_shipdate      CHAR(8),
  l_commitdate    CHAR(8),
  l_receiptdate   CHAR(8),
  l_shipinstruct  CHAR(25),
  l_shipmode      CHAR(10),
  l_orderstatus   CHAR(1),
  l_totalprice    FLOAT8,
  l_orderdate     CHAR(8),
  l_orderpriority CHAR(15),
  l_clerk         CHAR(15),
  l_shippriority  INTEGER,
  l_availqty      INTEGER,
  l_supplycost    FLOAT8 not null
);


CREATE OR REPLACE PROCEDURE suppliers_procedure()
returns string
language sql
as
$$
begin
MERGE INTO DATA_MART.d_supplier ds
USING (
  SELECT s_suppkey,
         s_name,
         s_address,
         n_name,
         r_name,
         s_phone,
         s_acctbal
  FROM CORE_DWH.suppliers_stream
  JOIN CORE_DWH.nation ON s_nationkey = n_nationkey
  JOIN CORE_DWH.region ON n_regionkey = r_regionkey
) supp
ON ds.s_suppkey = supp.s_suppkey
WHEN MATCHED THEN 
UPDATE SET ds.s_name = supp.s_name,
   ds.s_address = supp.s_address,
   ds.s_nation = supp.n_name,
   ds.s_region = supp.r_name,
   ds.s_phone = supp.s_phone,
   ds.s_acctbal = supp.s_acctbal
WHEN NOT MATCHED THEN
INSERT (s_suppkey,
        s_name,
        s_address,
        s_nation,
        s_region,
        s_phone, 
        s_acctbal)
VALUES (supp.s_suppkey,
       supp.s_name,
       supp.s_address,
       supp.n_name,
       supp.r_name,
       supp.s_phone,
       s_acctbal);
    return 'Completed';
end;
$$
;       


CREATE OR REPLACE PROCEDURE parts_procedure()
returns string
language sql
as
$$
begin
MERGE INTO DATA_MART.d_part dp
USING (
  SELECT p_partkey,
  p_name,
  p_mfgr,
  p_brand,
  p_type,
  p_size,
  p_container,
  p_retailprice
  FROM CORE_DWH.parts_stream
) part
ON dp.p_partkey = part.p_partkey
WHEN MATCHED THEN 
UPDATE SET dp.p_name = part.p_name,
   dp.p_mfgr = part.p_mfgr,
   dp.p_brand = part.p_brand,
   dp.p_type = part.p_type,
   dp.p_size = part.p_size,
   dp.p_container = part.p_container,
   dp.p_retailprice = part.p_retailprice
WHEN NOT MATCHED THEN
INSERT (p_partkey,
        p_name,
        p_mfgr,
        p_brand,
        p_type,
        p_size, 
        p_container,
        p_retailprice)
VALUES (part.p_partkey,
        part.p_name,
        part.p_mfgr,
        part.p_brand,
        part.p_type,
        part.p_size, 
        part.p_container,
        part.p_retailprice);
    return 'Completed';
end;
$$
;       


CREATE OR REPLACE PROCEDURE customers_procedure()
returns string
language sql
as
$$
begin
MERGE INTO DATA_MART.d_customer dc
USING (
  SELECT c_custkey,
         c_name,
         c_address,
         n_name,
         c_phone,
         c_acctbal,
         c_mktsegment
  FROM CORE_DWH.customers_stream
  JOIN CORE_DWH.nation ON c_nationkey = n_nationkey
) cust
ON dc.c_custkey = cust.c_custkey
WHEN MATCHED THEN 
UPDATE SET dc.c_name = cust.c_name,
   dc.c_address = cust.c_address,
   dc.c_nation = cust.n_name,
   dc.c_phone = cust.c_phone,
   dc.c_acctbal = cust.c_acctbal,
   dc.c_mktsegment = cust.c_mktsegment
WHEN NOT MATCHED THEN
INSERT (c_custkey,
        c_name,
        c_address,
        c_nation,
        c_phone,
        c_acctbal, 
        c_mktsegment)
VALUES (cust.c_custkey,
        cust.c_name,
        cust.c_address,
        cust.n_name,
        cust.c_phone,
        cust.c_acctbal, 
        cust.c_mktsegment);
    return 'Completed';
end;
$$
;


CREATE OR REPLACE PROCEDURE lineitems_procedure()
returns string
language sql
as
$$
begin
MERGE INTO DATA_MART.f_lineitem fl
USING (
  SELECT l_orderkey,
         l_partkey,
         l_suppkey,
         l_linenumber,
         c_custkey,
         l_quantity,
         l_extendedprice,
         l_discount,
         l_tax,
         l_returnflag,
         REPLACE(REPLACE(TO_CHAR(l_shipdate), ' 00:00:00.000', ''), '-', '') l_shipdate,
         REPLACE(REPLACE(TO_CHAR(l_commitdate), ' 00:00:00.000', ''), '-', '') l_commitdate,
         REPLACE(REPLACE(TO_CHAR(l_receiptdate), ' 00:00:00.000', ''), '-', '') l_receiptdate,
         l_shipinstruct,
         l_shipmode,
         o_orderstatus,
         o_totalprice,
         REPLACE(REPLACE(TO_CHAR(o_orderdate), ' 00:00:00.000', ''), '-', '') o_orderdate,
         o_orderpriority,
         o_clerk,
         o_shippriority,
         ps_availqty,
         ps_supplycost
  FROM CORE_DWH.lineitems_stream
  JOIN CORE_DWH.orders_stream ON l_orderkey = o_orderkey
  JOIN CORE_DWH.partsupp_stream ON l_partkey = ps_partkey AND l_suppkey = ps_suppkey
  JOIN CORE_DWH.customer ON o_custkey = c_custkey
) line
ON fl.l_orderkey = line.l_orderkey AND fl.l_linenumber = line.l_linenumber
WHEN MATCHED THEN 
UPDATE SET fl.l_partkey = line.l_partkey,
  fl.l_suppkey = line.l_suppkey,
  fl.l_custkey = line.c_custkey,
  fl.l_quantity = line.l_quantity,
  fl.l_extendedprice = line.l_extendedprice,
  fl.l_discount = line.l_discount,
  fl.l_tax = line.l_tax,
  fl.l_returnflag = line.l_returnflag,
  fl.l_shipdate = line.l_shipdate,
  fl.l_commitdate = line.l_commitdate,
  fl.l_receiptdate = line.l_receiptdate,
  fl.l_shipinstruct = line.l_shipinstruct,
  fl.l_shipmode = line.l_shipmode,
  fl.l_orderstatus = line.o_orderstatus,
  fl.l_totalprice = line.o_totalprice,
  fl.l_orderdate = line.o_orderdate,
  fl.l_orderpriority = line.o_orderpriority,
  fl.l_clerk = line.o_clerk,
  fl.l_shippriority = line.o_shippriority,
  fl.l_availqty = line.ps_availqty,
  fl.l_supplycost = line.ps_supplycost   
WHEN NOT MATCHED THEN
INSERT (l_orderkey,
        l_partkey,
        l_suppkey,
        l_linenumber,
        l_custkey,
        l_quantity,
        l_extendedprice,
        l_discount,
        l_tax,
        l_returnflag,
        l_shipdate,
        l_commitdate,
        l_receiptdate,
        l_shipinstruct,
        l_shipmode,
        l_orderstatus,
        l_totalprice,
        l_orderdate,
        l_orderpriority,
        l_clerk,
        l_shippriority,
        l_availqty,
        l_supplycost)
VALUES (line.l_orderkey,
        line.l_partkey,
        line.l_suppkey,
        line.l_linenumber,
        line.c_custkey,
        line.l_quantity,
        line.l_extendedprice,
        line.l_discount,
        line.l_tax,
        line.l_returnflag,
        line.l_shipdate,
        line.l_commitdate,
        line.l_receiptdate,
        line.l_shipinstruct,
        line.l_shipmode,
        line.o_orderstatus,
        line.o_totalprice,
        line.o_orderdate,
        line.o_orderpriority,
        line.o_clerk,
        line.o_shippriority,
        line.ps_availqty,
        line.ps_supplycost);
    return 'Completed';
end;
$$
;


CREATE OR REPLACE TASK tsk_master
  WAREHOUSE = compute_wh
  SCHEDULE = '1 minute'
WHEN 
  SYSTEM$STREAM_HAS_DATA('CORE_DWH.lineitems_stream')
AS SELECT('Give a start to the rest of the tasks');;;


CREATE OR REPLACE TASK tsk_Customer
    WAREHOUSE  = compute_wh
    AFTER tsk_master
AS CALL customers_procedure();


CREATE OR REPLACE TASK tsk_Supplier
    WAREHOUSE  = compute_wh
    AFTER tsk_Customer
AS CALL suppliers_procedure();


CREATE OR REPLACE TASK tsk_Part
    WAREHOUSE  = compute_wh
    AFTER tsk_Supplier
AS CALL parts_procedure();


CREATE OR REPLACE TASK tsk_Lineitem
    WAREHOUSE  = compute_wh
    AFTER tsk_Part
AS CALL lineitems_procedure();


ALTER TASK tsk_Lineitem RESUME;
ALTER TASK tsk_Part RESUME;
ALTER TASK tsk_Supplier RESUME;
ALTER TASK tsk_Customer RESUME;
ALTER TASK tsk_master RESUME;

SHOW TASKS;
