use role securityadmin;
create role developer_role;
show roles;

--- creating user
create user dev_user1
password ='doe'
default_role = developer_role
must_change_password = false;

grant role developer_role to user dev_user1;

use role sysadmin;

create warehouse dev_warehouse
warehouse_size = 'SMALL'
auto_suspend = 60;

grant usage on warehouse dev_warehouse to role developer_role;

grant create database on account to role developer_role;
