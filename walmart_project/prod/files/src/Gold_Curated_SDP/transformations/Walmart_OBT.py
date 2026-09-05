from pyspark import pipelines as dp
from pyspark.sql.functions import *

expect_all_obt = {

    # Primary / foreign keys
    "rule_1": "order_item_id IS NOT NULL",
    "rule_2": "order_id IS NOT NULL",
    "rule_3": "product_id IS NOT NULL",
    "rule_4": "customer_id IS NOT NULL",
    "rule_5": "store_id IS NOT NULL",
    "rule_6": "employee_id IS NOT NULL",

    # Numeric validity
    "rule_7": "quantity > 0",
    "rule_8": "unit_price >= 0",
    "rule_9": "line_amount >= 0",
    "rule_10": "total_amount >= 0",
    "rule_11": "product_price >= 0",
    "rule_12": "employee_salary >= 0",

    # Timestamp validity
    "rule_13": "order_timestamp IS NOT NULL",
    "rule_14": "order_timestamp >= TIMESTAMP '2010-01-01 00:00:00'",

    # Product completeness
    "rule_15": "product_name IS NOT NULL",
    "rule_16": "category IS NOT NULL",

    # Customer completeness
    "rule_17": "customer_firstname IS NOT NULL",
    "rule_18": "customer_lastname IS NOT NULL",
    "rule_19": "customer_email IS NOT NULL",

    # Store completeness
    "rule_20": "store_name IS NOT NULL",
    "rule_21": "store_city IS NOT NULL",
    "rule_22": "store_country IS NOT NULL",

    # Employee completeness
    "rule_23": "employee_firstname IS NOT NULL",
    "rule_24": "employee_lastname IS NOT NULL",
    "rule_25": "employee_jobtitle IS NOT NULL"
}

@dp.view(
    name="one_big_table_silver_view",
    comment="One big table with unique columns consolidated from all walmart_catalog.silver tables"
)
def one_big_table_silver_view():
    orders = spark.read.table("walmart_catalog.silver.orders").alias("orders")
    order_items = spark.readStream.table("walmart_catalog.silver.order_items").alias("order_items")
    products = spark.read.table("walmart_catalog.silver.products").alias("products")
    employees = spark.read.table("walmart_catalog.silver.employees").alias("employees")
    customers = spark.read.table("walmart_catalog.silver.customers").alias("customers")
    stores = spark.read.table("walmart_catalog.silver.stores").alias("stores")

    # Join tables and select only unique columns (no dual IDs, dual timestamps)
    df = (order_items.join(orders, col("order_items.order_id") == col("orders.order_id"), "left")\
        .join(products, col("order_items.product_id") == col("products.product_id"), "left")\
        .join(customers, col("orders.customer_id") == col("customers.customer_id"), "left")\
        .join(stores, col("orders.store_id") == col("stores.store_id"), "left")\
        .join(employees, col("stores.store_id") == col("employees.store_id"), "left")\
        .select(col("order_items.order_item_id"),col("order_items.order_id"),col("order_items.product_id"),col("order_items.quantity"),col("order_items.unit_price"),col("order_items.line_amount"),col("orders.customer_id"), col("orders.order_timestamp"), col("orders.total_amount"),col("products.product_name"), col("products.category"), col("products.price").alias("product_price"), col("customers.customer_firstname"),col("customers.customer_lastname"),col("customers.customer_email"),col("customers.customer_phone"),col("customers.customer_city"),col("customers.customer_province"),col("customers.customer_country"),col("stores.store_id"),col("stores.store_name"),col("stores.store_city"),col("stores.store_province"),col("stores.store_country"),col("stores.store_number"),col("employees.employee_id"),col("employees.employee_firstname"),col("employees.employee_lastname"),col("employees.employee_jobtitle"),col("employees.employee_salary")))
    
    df = df.withColumn("last_updated_timestamp", current_timestamp())
    return df


dp.create_streaming_table("Walmart_Obt", comment="Final Obt table", table_properties={"pipelines.autoOptimize.managed": "true"}, expect_all_or_fail=(expect_all_obt))

dp.create_auto_cdc_flow(
    target = "Walmart_Obt",
    source = "one_big_table_silver_view",
    keys = ["order_item_id"],
    sequence_by = "last_updated_timestamp",
    stored_as_scd_type = "1",
    track_history_except_column_list = None,
    except_column_list = ["last_updated_timestamp"],
    name = None,
    once = False
)