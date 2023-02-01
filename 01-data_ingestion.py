# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest Data 

# COMMAND ----------

#dbutils.fs.mounts()


# COMMAND ----------

#check files
%fs
ls /mnt/f1storage2023/raw

# COMMAND ----------

## Create files

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType

circuits_schema=StructType(fields=[
                    StructField("circuitId",IntegerType(),False),
                    StructField("circuitRef",StringType(),True),
                    StructField("name",IntegerType(),True),
                    StructField("location",IntegerType(),True),
                    StructField("country",IntegerType(),True),
                    StructField("lat",DoubleType(),True),
                    StructField("lng",DoubleType(),True),
                    StructField("alt",IntegerType(),True),
                    StructField("url",StringType(),False)
])

# COMMAND ----------

circuits_df=spark.read.option("header","true")\
        .schema(circuits_schema)\
        .csv("dbfs:/mnt/f1storage2023/raw/circuits.csv")


# COMMAND ----------

display(circuits_df)
