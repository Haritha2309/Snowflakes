from snowflake.snowpark import Session
from snowflake.snowpark.functions import col,avg,row_number
 
def initiateSession(): 
    connection_parameters = {
            "account": "eguaepf-tm71891",
            "user": "dev_user1",
            "password": "StrongP@ssw0rd!",
            "role": "developer_role", 
            "warehouse": "dev_warehouse",
            "database": "dev_db_test",
            "schema":"snowpark_schema"
    }
    session = Session.builder.configs(connection_parameters).create()
    return session
 
session = initiateSession()

df_cust=session.sql("select * from dev_db_test.public.my_customer")
df_cust.show()
 
 
#####################create a dataframe using session.table
df_orders = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS")
df_orders.show(5)

df_orders.groupBy("O_ORDERSTATUS").avg("O_TOTALPRICE").show()


df_orders_table=df_orders.groupBy("O_ORDERSTATUS").avg("O_TOTALPRICE")

df_orders_table.write.mode("overwrite").save_as_table("orders_saved_table", table_type="transient")

from snowflake.snowpark.types import StructField, StructType, IntegerType, StringType, VariantType,DateType
 
schema = StructType(
    [StructField("CUSTOMER_ID", IntegerType()),
     StructField("CUSTOMER_NAME", StringType()),
     StructField("CUSTOMER_EMAIL", StringType()),
     StructField("CUSTOMER_CITY", StringType()),
     StructField("CUSTOMER_STATE", StringType()),
     StructField("CUSTOMER_DOB", DateType())
    ])


df_customer = session.read.schema(schema).options({"field_delimiter": "|", "skip_header": 1}).csv('@my_snowpark_stage')
df_customer.show(5)
df_customer.copy_into_table("customer_snowpark_aws_table")