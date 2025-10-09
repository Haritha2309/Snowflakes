select current_role();
select current_user();

use warehouse dev_warehouse;
create database project_db;

show databases;

create table mytable(
name varchar
);

insert into mytable values
('SNOWFLAKE  USER 1');

select * from mytable;

drop table mytable;

---------

use role developer_role;
use database project_db;
use schema public;
use warehouse dev_warehouse;

create or replace stage my_azure_storage
url = 'azure://harithahex.blob.core.windows.net/mycontainer1'
credentials = (AZURE_SAS_TOKEN = 'sv=2024-11-04&ss=bfqt&srt=co&sp=rwdlacupiytfx&se=2025-10-30T12:05:41Z&st=2025-10-08T03:50:41Z&spr=https&sig=zcHNcGVhGMpYorbiriPenTG%2BWVEmJ1xlB2vQgnDSM7Q%3D')

list @my_azure_storage;

use role developer_role;
use database project_db;
use schema public;
use warehouse dev_warehouse;
 
create or replace stage my_azure_stage
url = 'azure://harithahex.blob.core.windows.net/mycontainer1'
credentials = (AZURE_SAS_TOKEN = 'sv=2024-11-04&ss=bfqt&srt=co&sp=rwdlacupiytfx&se=2025-10-30T12:44:21Z&st=2025-10-08T04:29:21Z&spr=https&sig=938udKUnTwFAZM0YHhGkqaZyMc9fBJ5sAy5O%2BEE6hxY%3D');
 
--see what is in the stage??
list @my_azure_stage;

select $1,$2,$3,$4 from @my_azure_stage limit 100;
select $1,$2,$3,$4,metadata$filename from @my_azure_stage limit 100;

create table mylineitem 
like SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM;

select * from mylineitem;

copy into mylineitem from @my_azure_stage;

select count(*) from mylineitem;

create stage my_aws_public_stage
url = 's3://general-bkt-snfdata/datalake/';
 
list @my_aws_public_stage;

create table mylineitem2
like SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM;

copy into mylineitem2
from @my_aws_public_stage;

--- files = ('file.csv','file2.csv')
-- pattern('.csv')

select * from PROJECT_DB.INFORMATION_SCHEMA.LOAD_HISTORY limit 10 ;

select * from PROJECT_DB.INFORMATION_SCHEMA.STAGES;

select * from PROJECT_DB.INFORMATION_SCHEMA.TABLES;

---------

create or replace stage my_azure_stage2
url = 'azure://harithahex.blob.core.windows.net/mycontainer2'
credentials = (AZURE_SAS_TOKEN = 'sv=2024-11-04&ss=bfqt&srt=co&sp=rwdlacupiytfx&se=2025-10-30T12:44:21Z&st=2025-10-08T04:29:21Z&spr=https&sig=938udKUnTwFAZM0YHhGkqaZyMc9fBJ5sAy5O%2BEE6hxY%3D');

list @my_azure_stage2;

select $1,$2,$3,$4 from @my_azure_stage2 limit 10;

create table my_customer (
Customer_ID number,
Customer_Name varchar,
Customer_Email varchar,
Customer_City varchar,
Customer_State varchar,
Customer_DOB date
);

copy into my_customer
from @my_azure_stage2;

desc stage my_azure_stage2;

create file format my_custom_csv1
type = csv
FIELD_DELIMITER = '|'
SKIP_HEADER = 1;

copy into my_customer
from @my_azure_stage2
file_format = my_custom_csv1;

select * from my_customer;

create table my_customer_subset(
Customer_ID number,
Customer_Name varchar,
Customer_Email varchar
)

copy into my_customer_subset
from @my_azure_stage2
file_format = my_custom_csv1;

desc stage my_azure_stage2;

create file format my_custom_csv2
type = csv
FIELD_DELIMITER = '|'
SKIP_HEADER = 1
ERROR_ON_COLUMN_COUNT_MISMATCH = false;

copy into my_customer_subset
from @my_azure_stage2
file_format = my_custom_csv2;

select * from my_customer_subset

------- reloading 

create or replace  table my_customer (
Customer_ID number,
Customer_Name varchar,
Customer_Email varchar,
Customer_City varchar,
Customer_State varchar,
Customer_DOB date
);

select count(*) from my_customer -- 0

copy into my_customer
from @my_azure_stage2
file_format = my_custom_csv1;

select count(*) from my_customer  -- 436 

copy into my_customer
from @my_azure_stage2
file_format = my_custom_csv1
force = true;

select count(*) from my_customer -- 872

--- purge 

create or replace  table my_customer (
Customer_ID number,
Customer_Name varchar,
Customer_Email varchar,
Customer_City varchar,
Customer_State varchar,
Customer_DOB date
);

copy into my_customer
from @my_azure_stage2
file_format = my_custom_csv1
purge = true

--- specific columns used

create table my_customer_subset2 (
Customer_ID number,
Customer_Name varchar,
DOB date
);
 
copy into my_customer_subset2
from @my_azure_stage2
file_format = my_custom_csv2; --fail
 
copy into my_customer_subset2
from 
(
select $1, $2, $6 from @my_azure_stage2
)
file_format = my_custom_csv2;
 
select * from my_customer_subset2;

---------------------
select * from PROJECT_DB.PUBLIC.MY_CUSTOMER_BATCH;

create table MY_CUSTOMER_BATCH2 like MY_CUSTOMER_BATCH;

select count(*) from MY_CUSTOMER_BATCH;


create or replace stage my_azure_teststage
url = 'azure://harithahex.blob.core.windows.net/testcontainer'
credentials = (AZURE_SAS_TOKEN = 'sv=2024-11-04&ss=bfqt&srt=co&sp=rwdlacupiytfx&se=2025-10-30T12:44:21Z&st=2025-10-08T04:29:21Z&spr=https&sig=938udKUnTwFAZM0YHhGkqaZyMc9fBJ5sAy5O%2BEE6hxY%3D');

list @my_azure_teststage;

create or replace table emp (
         first_name string ,
         last_name string ,
         email string ,
         streetaddress string ,
         city string ,
         start_date date
);

copy into emp
from @my_azure_teststage;

select * from emp;

--- data load validation

copy into emp
from @my_azure_teststage
validation_mode = 'return_errors';

-------- error overriding 

copy into emp 
from @my_azure_teststage
ON_ERROR = 'CONTINUE'

CREATE OR REPLACE TABLE rejected_records AS
SELECT * FROM TABLE(VALIDATE(emp,JOB_ID =>'_last'));

select * from emp;
select * from rejected_records;

COPY INTO @my_azure_teststage/rejected_records.csv
FROM rejected_records
file_format = (TYPE = 'CSV')

--- JSON 
create or replace stage my_azure_jsonstage
url = 'azure://harithahex.blob.core.windows.net/jsoncontainer'
credentials = (AZURE_SAS_TOKEN = 'sv=2024-11-04&ss=bfqt&srt=co&sp=rwdlacupiytfx&se=2025-10-30T12:44:21Z&st=2025-10-08T04:29:21Z&spr=https&sig=938udKUnTwFAZM0YHhGkqaZyMc9fBJ5sAy5O%2BEE6hxY%3D')
file_format = (type = json);

desc stage my_azure_jsonstage;

list @my_azure_jsonstage;

select $1 from @my_azure_jsonstage;

create table json_raw(
mycolumn1 variant -- store anytype of data
);

select * from json_raw;

copy into json_raw 
from @my_azure_jsonstage;

select * from json_raw;

---- another way
create or replace table home_sales(
city varchar ,
postal_code varchar,
sq_ft number,
sale_date date,
price number,
data_file varchar,
load_date_time timestamp
);

copy into home_sales(city,postal_code,sq_ft,sale_date,price,data_file,load_date_time)
from (
select $1:location.city::varchar,
$1:location.zip::varchar,
$1:dimensions.sq_ft::number,
$1:sale_date::date,
$1:price::number,
metadata$filename,
current_timestamp 
from @my_azure_jsonstage/sales2.json
);

select * from home_sales;

-----nested
create or replace table nested_json_raw(
json_data_raw variant
);

copy into nested_json_raw
from @my_azure_jsonstage/nested.json;

select * from nested_json_raw;

create or replace  table organisation(
org_name string,
state string,
org_code string,
extract_date  date
);

INSERT INTO organisation
SELECT
    VALUE:name::String AS org_name,
    VALUE:state::String AS state,
    VALUE:org_code::String AS org_code,
    json_data_raw:extract_date AS extract_date
FROM
    nested_json_raw,lateral flatten( input => json_data_raw:organisations) ;

select * from organisation;
