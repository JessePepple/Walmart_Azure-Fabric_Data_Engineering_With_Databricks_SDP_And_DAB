from pyspark import pipelines as dp
from pyspark.sql.functions import *

expect_products = {
     "rule_1": "product_id IS NOT NULL",
    "rule_2": "product_name IS NOT NULL",
    "rule_3": "category IS NOT NULL",
    "rule_4": "brand IS NOT NULL",
    "rule_5": "price IS NOT NULL",
    "rule_6": "product_flag IS NOT NULL",
    "rule_7": "product_id > 0",
    "rule_8": "price >= 0",
}

@dp.view(
    name= "Dim_Products_stg",
    comment= "View for our DimProducts"
)

def Dim_Products_stg():
    df = spark.readStream.table("walmart_catalog.silver.products")
    return df



dp.create_streaming_table(name="Dim_Products", comment="Empty table for our DimProducts and SCD TYPE 2 Implementation", table_properties={"pipelines.autoOptimize.managed": "true"}, expect_all_or_fail=(expect_products))

dp.create_auto_cdc_flow(
    target = "Dim_Products",
    source = "Dim_Products_stg",
    keys = ["product_id"],
    sequence_by = "last_updated_timestamp",
    stored_as_scd_type = "2",
    track_history_except_column_list = None,
    except_column_list = ["last_updated_timestamp"],
    name = None,
    once = False
)



