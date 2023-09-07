# Databricks notebook source
input_stream_path="dbfs:/mnt/nlyadls/raw/input_stream/"

# COMMAND ----------

output="dbfs:/mnt/nlyadls/raw/output_stream"

# COMMAND ----------

(
 spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.schemaLocation",f"{output}/Shardul/autoloader/1/schemalocation")
.load(input_stream_path)
.writeStream
.option("checkpointLocation", f"{output}/Shardul/autoloader/1/checkpoint")
.option("path",f"{output}/Shardul/autoloader/1/table1")
.table("stream.autoloader1")
)

# COMMAND ----------

(
spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.schemaLocation",f"{output}/Shardul/autoloader/2/schemalocation")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaEvolutionMode","rescue")
.load(input_stream_path)
.writeStream
.option("checkpointLocation", f"{output}/Shardul/autoloader/2/checkpoint")
.option("path",f"{output}/Shardul/autoloader/2/table1")
.option("mergeSchema", "true")
.table("stream.autoloader2")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stream.autoloader2

# COMMAND ----------

create schema if not exists iot

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC input_stream_path="dbfs:/mnt/nlyadls/raw/iot_data/"
# MAGIC
# MAGIC output="dbfs:/mnt/nlyadls/raw/iot_data_output"

# COMMAND ----------

create schema if not exists iot

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC (
# MAGIC
# MAGIC spark.readStream
# MAGIC
# MAGIC .format("cloudFiles")
# MAGIC
# MAGIC .option("cloudFiles.format", "json")
# MAGIC
# MAGIC .option("cloudFiles.schemaLocation",f"{output}/Shardul/iot/bronze/schema")
# MAGIC
# MAGIC .option("cloudFiles.inferColumnTypes",True)
# MAGIC
# MAGIC .load(input_stream_path)
# MAGIC
# MAGIC .writeStream
# MAGIC
# MAGIC .option("checkpointLocation", f"{output}/Shardul/iot/bronze/checkpoint")
# MAGIC
# MAGIC .option("path",f"{output}/Shardul/iot/bronze/bronze_files")
# MAGIC
# MAGIC .trigger(once=True)
# MAGIC
# MAGIC .option("mergeSchema", "true")
# MAGIC
# MAGIC .table("iot.bronze")
# MAGIC
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists iot

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC (
# MAGIC
# MAGIC spark.readStream
# MAGIC
# MAGIC .format("cloudFiles")
# MAGIC
# MAGIC .option("cloudFiles.format", "json")
# MAGIC
# MAGIC .option("cloudFiles.schemaLocation",f"{output}/Shardul/iot/bronze/schema")
# MAGIC
# MAGIC .option("cloudFiles.inferColumnTypes",True)
# MAGIC
# MAGIC .load(input_stream_path)
# MAGIC
# MAGIC .writeStream
# MAGIC
# MAGIC .option("checkpointLocation", f"{output}/Shardul/iot/bronze/checkpoint")
# MAGIC
# MAGIC .option("path",f"{output}/Shardul/iot/bronze/bronze_files")
# MAGIC
# MAGIC .trigger(once=True)
# MAGIC
# MAGIC .option("mergeSchema", "true")
# MAGIC
# MAGIC .table("iot.bronze")
# MAGIC
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from iot.bronze

# COMMAND ----------


