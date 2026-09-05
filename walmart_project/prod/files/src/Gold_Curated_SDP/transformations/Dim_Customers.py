from pyspark import pipelines as dp
from pyspark.sql.functions import *

expect_customers = {
     "rule_1": "customer_id IS NOT NULL",
    "rule_2": "customer_firstname IS NOT NULL",
    "rule_3": "customer_lastname IS NOT NULL",
    "rule_4": "customer_email IS NOT NULL",
    "rule_5": "customer_phone IS NOT NULL",
    "rule_6": "customer_city IS NOT NULL",
    "rule_7": "customer_province IS NOT NULL",
    "rule_8" : "customer_country IS NOT NULL"
}

@dp.view(
    name= "Dim_Customers_stg",
    comment= "View for our DimCustomers"
)

def Dim_Customers_stg():
    df = spark.readStream.table("walmart_catalog.silver.customers")
    return df



dp.create_streaming_table(name="Dim_Customers", comment="Empty table for our DimCustomers and SCD TYPE 2 Implementation", table_properties={"pipelines.autoOptimize.managed": "true"}, expect_all_or_fail=(expect_customers))

dp.create_auto_cdc_flow(
    target = "Dim_Customers",
    source = "Dim_Customers_stg",
    keys = ["customer_id"],
    sequence_by = "last_updated_timestamp",
    stored_as_scd_type = "2",
    track_history_except_column_list = None,
    except_column_list = ["last_updated_timestamp"],
    name = None,
    once = False
)
