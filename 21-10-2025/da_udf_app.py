from snowflake.snowpark import Session
from snowflake.snowpark.functions import col,avg,row_number
from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import *
 
 
def initiateSession():
   
    connection_parameters = {
            "account": "eguaepf-tm71891",
            "user": "dev_user1",
            "password": "StrongP@ssw0rd!",
            "role": "developer_role", 
            "warehouse": "dev_warehouse",
            "database": "dev_db_test",
            "schema":"udf_sch"
    }
    session = Session.builder.configs(connection_parameters).create()
    return session
 
session = initiateSession()


def calculate_deductions(total_price,deduction_percentage):
  return ((deduction_percentage)/100)*total_price

#UDF to deduct percentage from total price and returns the price
####register (deploy) the UDF via our internal stage
session.udf.register(
    func = calculate_deductions,
    return_type = FloatType(),
    input_types = [FloatType(),FloatType()],
    is_permanent = True,
    name = 'calculate_deductions',
    replace = True,
    stage_location = '@my_udf_stage'
)
sql = """select
            O_TOTALPRICE,
            calculate_deductions(O_TOTALPRICE,30)
        from
            SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS limit 10
        """
session.sql(sql).show()
 