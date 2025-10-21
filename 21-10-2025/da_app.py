from snowflake.snowpark import Session
from snowflake.snowpark.functions import col,avg,row_number,count_distinct
 
 
def initiateSession():
   
    connection_parameters = {
            "account": "eguaepf-tm71891",
            "user": "da_user1",
            "password": "dau",
            "role": "data_analyst_role",
            "warehouse": "da_warehouse",
            "database": "parking_project",
            "schema":"public"
    }
    session = Session.builder.configs(connection_parameters).create()
    return session
 
session = initiateSession()
 
session.sql("show tables").collect()
 
parking_df = session.table("NY_PARKING_TABLE")
 
# --Group by VEHICLE_TYPE and count summons ---
summons_count_df = (
    parking_df
    .group_by(col("VEHICLE_MAKE"))
    .agg(count_distinct(col("SUMMONS_NUMBER")).alias("TOTAL_SUMMONS"))
)
 
#  Show results ---
summons_count_df.show(100)
summons_count_df_sorted = summons_count_df.sort(col("TOTAL_SUMMONS").desc())
summons_count_df_sorted.show()

