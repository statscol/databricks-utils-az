# Databricks notebook source
# MAGIC %md
# MAGIC ## Assignment Races

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read File

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/f1storage2023/raw

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType

races_schema=StructType(fields=[
                    StructField("raceId",IntegerType(),False),
                    StructField("year",IntegerType(),True),
                    StructField("round",IntegerType(),True),
                    StructField("circuitId",IntegerType(),False),
                    StructField("name",StringType(),True),
                    StructField("date",StringType(),True),
                    StructField("time",StringType(),True),
                    StructField("url",StringType(),True)
                    ])

races_df=spark.read.option("header","true").\
        schema(races_schema).\
        csv("/mnt/f1storage2023/raw/races.csv")

races_df.printSchema()

# COMMAND ----------

races_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform data

# COMMAND ----------

from pyspark.sql.functions import col,lit,to_timestamp,concat_ws,current_timestamp,regexp_replace,udf
import re

@udf(returnType=StringType()) 
def process_time(text:str):
    text=text.lower().replace(r'\n',"00:00:00")
    return text


# COMMAND ----------

races_df_renamed=races_df.withColumnRenamed("raceId","race_id").\
                withColumnRenamed("year","race_year").\
                withColumnRenamed("circuitId","circuit_id").\
                withColumn("time_mod",process_time(col("time"))).\
                withColumn("race_timestamp",to_timestamp(concat_ws(" ",col("date"),col("time_mod")),format="yyyy-MM-dd HH:mm:ss")).\
                withColumn("ingestion_date",current_timestamp())

races_df_renamed.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save to HDFS

# COMMAND ----------

races_df_renamed.write.\
        mode("overwrite").\
        partitionBy("round").\
        parquet("/mnt/f1storage2023/processed/races")


# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls /mnt/f1storage2023/processed/races

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Validate file

# COMMAND ----------

df_races_saved=spark.read.parquet("/mnt/f1storage2023/processed/races")
df_races_saved.show()
