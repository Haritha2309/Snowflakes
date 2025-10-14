use role developer_role;
use database project_db;
create schema tables_sch;
use schema tables_sch;
use warehouse dev_warehouse;
-----------------------
create or replace stage my_tables_az_stage
url = 'azure://harithahex.blob.core.windows.net/mycontainer1'
credentials = (AZURE_SAS_TOKEN = 'sv=2024-11-04&ss=bfqt&srt=co&sp=rwdlacupiytfx&se=2025-10-28T15:13:43Z&st=2025-10-14T06:58:43Z&spr=https&sig=swt68qZIIAXZy2IjsA4UqBjJB8OEHCEJ5PT2x%2BSuORY%3D')
list @my_tables_az_stage

------------------
create or replace external table ext_lineitems_t 
with location=@my_tables_az_stage
auto_refresh=false
file_format=(type=csv);

select * from ext_lineitems_t limit 10;

-----------------------------
select value:"c1",value:"c2" from ext_lineitems_t limit 10;
-------------------------
----lets recreate this table with proper column names for our data analysis----------

CREATE or replace EXTERNAL TABLE ext_lineitems_t
(
        ORDERKEY number AS (value:c1::number),
        PARTKEY number AS (value:c2::number),
        SUPPKEY number AS (value:c3::number),
        LINENUMBER number AS (value:c4::number),
        QTY number AS (value:c5::number),
        PRICE number AS (value:c6::number)
)
  with location=@my_tables_az_stage
  auto_refresh = false
  file_format = (type = csv);
 
  select * from ext_lineitems_t limit 10;
 
  select ORDERKEY, QTY from ext_lineitems_t limit 10;
 
  select ORDERKEY, QTY from ext_lineitems_t
  where QTY < 20;

  select count(*)from ext_lineitems_t;

  alter external table ext_lineitems_t REFRESH;