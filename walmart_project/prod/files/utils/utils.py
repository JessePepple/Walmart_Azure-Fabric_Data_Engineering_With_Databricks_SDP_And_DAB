from pyspark.sql.types import *
from pyspark.sql.functions import *



class SilverTransformation:
    def __init__(self, df=None):
        self.df = df

    def read_bronzedata(self, dataset):
        self.df = spark.read.table(f"walmart_catalog.bronze.{dataset}")
        return self.df

    def add_currtimestamp(self, cdc_col):
        self.df = self.df.withColumn(cdc_col, current_timestamp())
        return self.df

    def fill_nulls(self):
        self.df = self.df.fillna(0)
        return self.df
    
    def fill_strnulls(self):
        self.df = self.df.fillna("N/A")
        return self.df
    
    def drop_specificcols(self, col_val):
        self.df = self.df.drop(*col_val)
        return self.df
    
    def split_values (self, col_val, existent_col, delimeter, index):
        self.df = self.df.withColumn(col_val, split(col(existent_col), delimeter)[index])
        return self.df
    

    def scd_type1(self, table_val, primary_key, cdc_column ):
        from delta.tables import DeltaTable

        if spark.catalog.tableExists(f"walmart_catalog.silver.{table_val}"):
            dlt_obj = DeltaTable.forName(spark, f"walmart_catalog.silver.{table_val}")
            dlt_obj.alias("t").merge(
                self.df.alias("s"),
                f"t.{primary_key} = s.{primary_key}",
            ).whenMatchedUpdateAll(condition=f"s.{cdc_column} > t.{cdc_column}")\
                .whenNotMatchedInsertAll()\
                .execute()
        else:
            self.df.write.format("delta").mode("append").option("path", f"abfss://silver@walmartlakejess.dfs.core.windows.net/{table_val}_Data/{table_val}").saveAsTable(f"walmart_catalog.silver.{table_val}")
        return self.df
    
    def drop_duplicatedata(self, primary_key):
        self.df = self.df.dropDuplicates([primary_key])
        return self.df
    
    def cast_intcols(self, int_cols):
        for cols in int_cols:
            self.df = self.df.withColumn(cols, col(cols).cast(IntegerType()))
        return self.df
    
    def cast_timestampcols(self, timestamp_cols):
        for cols in timestamp_cols:
            self.df = self.df.withColumn(cols, col(cols).cast(TimestampType()))
        return self.df

    def drop_specific_cols(self, data_values):
        self.df = self.df.drop(*data_values)
        return self.df
    
    def cast_doublecols(self, double_cols):
        for cols in double_cols:
            self.df = self.df.withColumn(cols, col(cols).cast(DoubleType()))
        return self.df
    
    def rename_columns(self, column_mapping: dict):
        for old_name, new_name in column_mapping.items():
            self.df = self.df.withColumnRenamed(old_name, new_name)
        return self.df
    