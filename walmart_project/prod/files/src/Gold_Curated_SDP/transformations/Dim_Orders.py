from pyspark.sql.functions import *
from pyspark import pipelines as dp

expect_orders = {
     "rule_1": "order_id IS NOT NULL",
    "rule_2": "customer_id IS NOT NULL",
    "rule_3": "store_id IS NOT NULL",
    "rule_4": "order_timestamp > TIMESTAMP '2010-01-01 00:00:00'",
    "rule_5" : "payment_method IS NOT NULL",
    "rule_6": "order_status IS NOT NULL",
    "rule_7" : "total_amount IS NOT NULL"
}


@dp.view(
    name= "Dim_Orders_stg",
    comment= "Order Transformation"
)

def Dim_Orders():
    df = spark.readStream.table("walmart_catalog.silver.orders")
    return df


dp.create_streaming_table(name= "Dim_Orders", comment=" Dim_Orders SCD TYPE 2 implementation on empty streaming table", table_properties={"pipelines.autoOptimize.managed": "true"}, expect_all=(expect_orders))

dp.create_auto_cdc_flow(
    target = "Dim_Orders",
    source = "Dim_Orders_stg",
    keys = ["order_id"],
    sequence_by = "last_updated_timestamp",
    stored_as_scd_type = "2",
    track_history_except_column_list = None,
    except_column_list = ["last_updated_timestamp"],
    name = None,
    once = False
)


