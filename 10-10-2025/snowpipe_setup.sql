use role developer_role;
use database project_db;
create schema snowpipe_sch;
use schema snowpipe_sch;

create table my_customer_streaming(
customer_ID number,
customer_Name varchar,
customer_Email varchar,
customer_City varchar,
customer_State varchar,
customer_DOB date 
);

create stage my_azure_streaming_stage
url = 'azure://hexsnowpipe.blob.core.windows.net/streamingcontainer'
credentials = (AZURE_SAS_TOKEN = 'sv=2024-11-04&ss=bfqt&srt=co&sp=rwdlacupiytfx&se=2025-10-10T13:53:35Z&st=2025-10-10T05:38:35Z&spr=https&sig=e9yiwtz1Lfffi3g5PvRmAw1oLiXdl%2BVXvu%2F8b4uEKXQ%3D');

list @my_azure_streaming_stage;
 
create file format my_custom_csv1
type = csv
field_delimiter = '|'
skip_header = 1;
 
 
create pipe my_snowpipe
auto_ingest = true
integration = MY_AZURE_NOTIFICATIONS
as
copy into my_customer_streaming
from @my_azure_streaming_stage
file_format = my_custom_csv1;
 
--monitor snowpipe status
SELECT SYSTEM$PIPE_STATUS('my_snowpipe');
 
--Testing Snowpipe Loading..
select * from my_customer_streaming;
select count(*) from my_customer_streaming;
 
select * from table(information_schema.copy_history(table_name=>'my_customer_streaming', 
                    start_time=> dateadd(hours, -1, current_timestamp())));
 
ALTER PIPE my_snowpipe SET PIPE_EXECUTION_PAUSED = true;   