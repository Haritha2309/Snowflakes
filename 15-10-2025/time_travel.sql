use role developer_role;
use warehouse dev_warehouse;
use database project_db;
create schema time_sch;
use schema time_sch;

create or replace stage my_azure_stage2
url = 'azure://harithahex.blob.core.windows.net/mycontainer2'
credentials = (AZURE_SAS_TOKEN = 'sv=2024-11-04&ss=bfqt&srt=co&sp=rwdlacupiytfx&se=2025-10-31T14:39:04Z&st=2025-10-13T06:24:04Z&spr=https&sig=hDs6nMwontaQjaSkuvFggbmMa1yYoRf721cLe8t%2BEY8%3D') ;
--------------------------------
show stage my

LIST @PROJECT_DB.PUBLIC.my_azure_stage2;


create  or replace table customer_t
like
PROJECT_DB.PUBLIC.MY_CUSTOMER;

show tables in schema time_sch;

alter table customer_t
set
data_retention_time_in_days = 7;

--------------------------------------

select current_timestamp();-----2025-10-15 05:08:31.964 +0000
alter session set timezone="UTC";
select current_timestamp();------2025-10-15 05:08:52.321 +0000
copy into customer_t
from @PROJECT_DB.PUBLIC.my_azure_stage2/generated_customer_data.csv
file_format = (type = csv field_delimiter = '|' skip_header = 1);

update customer_t
set CUSTOMER_STATE = 'New Delhi'
where CUSTOMER_STATE = 'New South Wales';----54 updated

--------2 nd file------2025-10-15 05:23:04.944 +0000
copy into customer_t
from @PROJECT_DB.PUBLIC.my_azure_stage2/generated_customer_data2.csv
file_format = (type = csv field_delimiter = '|' skip_header = 1);

select count(*)from customer_t ;
-------3 rdfile------2025-10-15 05:16:54.420 +0000
copy into customer_t
from @PROJECT_DB.PUBLIC.my_azure_stage2/generated_customer_data3.csv
file_format = (type = csv field_delimiter = '|' skip_header = 1);

----------------seeing the time line

select * from customer_t at(timestamp => '2025-10-15 05:24:54.420 +0000'::timestamp);

------------------time travelling with query id--------------
--q1  01bfb83a-0001-669b-000c-36020002f69e
--q2  01bfb842-0001-669b-000c-36020002f702
--q3  01bfb83c-0001-6699-000c-3602000371be
-- q4 01bfb85c-0001-669b-000c-36020002f786 --update

select * from customer_t at(statement => '01bfb83c-0001-6699-000c-3602000371be'); 

select * from customer_t before(statement => '01bfb83c-0001-6699-000c-3602000371be');

--------------recovering table----
create table recovered 
clone customer_t  before(statement => '01bfb85c-0001-669b-000c-36020002f786');
select * from recovered where CUSTOMER_STATE = 'New South Wales';

 
create table recovered1
clone customer_t  at(statement => '01bfb85c-0001-669b-000c-36020002f786');
select * from recovered1 where CUSTOMER_STATE = 'New Delhi';