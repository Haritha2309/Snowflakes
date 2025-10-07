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
url = 'azure://hxazurestorage.blob.core.windows.net/mycontainer1'
credentials = (AZURE_SAS_TOKEN = 'sv=2024-11-04&ss=bfqt&srt=co&sp=rwdlacupiytfx&se=2025-10-30T14:25:04Z&st=2025-10-07T06:10:04Z&spr=https&sig=AXLaOe8r0gf%2Fmgrz3TGE7kgNkRUFQUqVZUD21NBRH4Y%3D')

list @my_azure_storage;

use role developer_role;
use database project_db;
use schema public;
use warehouse dev_warehouse;
 
create or replace stage my_azure_stage
url = 'azure://hxwstoragegaccount.blob.core.windows.net/mycontainer1'
credentials = (AZURE_SAS_TOKEN = 'sv=2024-11-04&ss=bfqt&srt=co&sp=rwdlacupiytfx&se=2025-10-30T14:25:04Z&st=2025-10-07T06:10:04Z&spr=https&sig=AXLaOe8r0gf%2Fmgrz3TGE7kgNkRUFQUqVZUD21NBRH4Y%3D');
 
--see what is in the stage??
list @my_azure_stage;