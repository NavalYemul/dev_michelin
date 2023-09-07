# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/nlyadls/raw

# COMMAND ----------

df=spark.read.json("dbfs:/mnt/nlyadls/raw/complex.json")

# COMMAND ----------

display(df)

# COMMAND ----------

df=spark.read.option("multiline",True).json("dbfs:/mnt/nlyadls/raw/complex.json")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.withColumn("topping",explode("topping")).display()

# COMMAND ----------

df.withColumn("topping",explode("topping"))\
.withColumn("topping_id",col("topping.id"))\
.withColumn("topping_type",col("topping.type"))\
.drop("topping")\
.display()

# COMMAND ----------

df.withColumn("batters",explode("batters")).display()

# COMMAND ----------

df.withColumn("batters",explode("batters.batter")).withColumn("batters_id",col("batters.id"))\
.withColumn("batters_type",col("batters.type")).drop("batters").display()

# COMMAND ----------

dffinal=df.withColumn("batters",explode("batters.batter"))\
.withColumn("batters_id",col("batters.id"))\
.withColumn("batters_type",col("batters.type"))\
.drop("batters")\
.withColumn("topping",explode("topping"))\
.withColumn("topping_id",col("topping.id"))\
.withColumn("topping_type",col("topping.type"))\
.drop("topping")

# COMMAND ----------

dffinal.write.saveAsTable("complexjson")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from complexjson

# COMMAND ----------

df=spark.read.table("complexjson")

# COMMAND ----------


