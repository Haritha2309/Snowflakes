use role developer_role;
use warehouse dev_warehouse;
use database project_db;
create schema cdc_sch;
use schema cdc_sch;

create or replace table members (
  id number(8),
  name varchar(255),
  fee number(3)
);
 
create or replace stream member_check on table members;
 
select * from members;
select * from member_check;  --stream  -insert or update or delete record

 
create or replace table members_prod (
  id number(8),
  name varchar(255),
  fee number(3)
);

select * from members_prod;

insert into members (id,name,fee)
values
(1,'Joe',0);
 
select * from member_check;
 
insert into members_prod(id,name,fee) 
select id, name, fee from member_check 
where metadata$action = 'INSERT' and metadata$isupdate = 'FALSE';

insert into members (id,name,fee)
values
(2,'Jane',0),
(3,'George',0);
 
select * from member_check;
 
insert into members_prod(id,name,fee) 
select id, name, fee from member_check 
where metadata$action = 'INSERT' and metadata$isupdate = 'FALSE';

insert into members (id,name,fee)
values
(4,'Betty',0),
(5,'Sally',0);
 
select * from member_check;
 
insert into members_prod(id,name,fee) 
select id, name, fee from member_check 
where metadata$action = 'INSERT' and metadata$isupdate = 'FALSE';

-----------------------------------delete----------------
DELETE FROM members WHERE NAME = 'Sally';
 
select * from member_check;
 
DELETE FROM MEMBERS_PROD WHERE NAME IN (select DISTINCT NAME
                                from member_check
                                where METADATA$ACTION = 'DELETE' and METADATA$ISUPDATE = 'FALSE');

---- logic 
MERGE INTO members_prod a USING member_check b ON a.ID = b.ID
 WHEN MATCHED AND metadata$action = 'DELETE' AND metadata$isupdate = 'FALSE' 
   THEN DELETE
 WHEN MATCHED AND metadata$action = 'INSERT' AND metadata$isupdate = 'TRUE' 
   THEN UPDATE SET a.FEE = b. FEE, a.NAME = b.NAME
 WHEN NOT MATCHED AND metadata$action = 'INSERT' AND metadata$isupdate = 'FALSE'
   THEN INSERT (ID, NAME, FEE) VALUES (b.ID, b.NAME, b.FEE);