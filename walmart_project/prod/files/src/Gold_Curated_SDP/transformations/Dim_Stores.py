from pyspark.sql.functions import *
from pyspark import pipelines as dp

expect_stores = {
     "rule_1": "store_id IS NOT NULL",
    "rule_2": "store_name IS NOT NULL",
    "rule_3": "store_city IS NOT NULL",
    "rule_4": "store_province IS NOT NULL",
    "rule_5" : "store_country IS NOT NULL",
    "rule_6": "store_number IS NOT NULL"
}


@dp.view(
    name= "Dim_Stores_stg",
    comment= "Store Transformation"
)

def Dim_Stores():
    df = spark.readStream.table("walmart_catalog.silver.stores")
    return df


dp.create_streaming_table(name= "Dim_Stores", comment=" Dim_Stores SCD TYPE 2 implementation on empty streaming table", table_properties={"pipelines.autoOptimize.managed": "true"}, expect_all=(expect_stores))

dp.create_auto_cdc_flow(
    target = "Dim_Stores",
    source = "Dim_Stores_stg",
    keys = ["store_id"],
    sequence_by = "last_updated_timestamp",
    stored_as_scd_type = "2",
    track_history_except_column_list = None,
    except_column_list = ["last_updated_timestamp"],
    name = None,
    once = False
)


