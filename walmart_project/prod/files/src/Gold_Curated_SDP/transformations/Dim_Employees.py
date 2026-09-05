from pyspark.sql.functions import *
from pyspark import pipelines as dp

expect_employee = {
     "rule_1": "employee_id IS NOT NULL",
    "rule_2": "store_id IS NOT NULL",
    "rule_3": "employee_firstname IS NOT NULL",
    "rule_4": "employee_email IS NOT NULL",
    "rule_5" : "employee_lastname IS NOT NULL",
    "rule_6": "employee_jobtitle IS NOT NULL",
    "rule_7": "employee_salary IS NOT NULL",
}


@dp.view(
    name= "Dim_Employees_stg",
    comment= "Employee Transformation"
)

def Dim_Employees():
    df = spark.readStream.table("walmart_catalog.silver.employees")
    return df


dp.create_streaming_table(name= "Dim_Employees", comment="SCD TYPE 2 implementation on empty streaming table", table_properties={"pipelines.autoOptimize.managed": "true"}, expect_all=(expect_employee))

dp.create_auto_cdc_flow(
    target = "Dim_Employees",
    source = "Dim_Employees_stg",
    keys = ["employee_id"],
    sequence_by = "last_updated_timestamp",
    stored_as_scd_type = "2",
    track_history_except_column_list = None,
    except_column_list = ["last_updated_timestamp"],
    name = None,
    once = False
)


