-- Databricks notebook source
-- MAGIC %python
-- MAGIC input_stream_path="dbfs:/mnt/nlyadls/rawproject/input_files"
-- MAGIC output="dbfs:/mnt/nlyadls/rawproject/output_files"

-- COMMAND ----------

create schema if not exists etl

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.readStream\
-- MAGIC .format("cloudFiles")\
-- MAGIC .option("cloudFiles.format", "csv")\
-- MAGIC .option("cloudFiles.schemaLocation",f"{output}/naval/etl/bronze/schema")\
-- MAGIC .option("cloudFiles.inferColumnTypes",True)\
-- MAGIC .load(input_stream_path)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC new_columns=['timestamp',
-- MAGIC  'user_id',
-- MAGIC  'page_visited',
-- MAGIC  'action',
-- MAGIC  'device_type',
-- MAGIC  'location',
-- MAGIC  'referral_source',
-- MAGIC  '_rescued_data']

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df1=df.toDF(*new_columns)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (df1.writeStream
-- MAGIC .option("delta.columnMapping.mode","name")
-- MAGIC .option("checkpointLocation", f"{output}/naval/etl/bronze/checkpoint")
-- MAGIC .option("path",f"{output}/naval/etl/bronze/bronze_files")
-- MAGIC .trigger(once=True)
-- MAGIC .option("mergeSchema", "true")
-- MAGIC .table("etl.bronze")
-- MAGIC )

-- COMMAND ----------

select * from etl.bronze
