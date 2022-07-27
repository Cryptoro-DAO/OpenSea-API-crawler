# Databricks notebook source
# MAGIC %md
# MAGIC Incrementally load ... event data retrieved from OpenSea between 2022-07-24 and 2022-07-26.
# MAGIC 
# MAGIC Total number of objects | Total size
# MAGIC -- | --
# MAGIC 195,356 | 14.8 GB

# COMMAND ----------

# MAGIC %fs ls /mnt/opensea-sg/lz/asset_events/20220724

# COMMAND ----------



# COMMAND ----------

spark.conf.get('spark.sql.jsonGenerator.ignoreNullFields')

# COMMAND ----------

base_location = '/mnt/opensea-sg/lz/asset_events/20220724'
# df = spark.read.option('recursiveFileLookup', 'true').json(base_location)
df = spark.read.options(recursiveFileLookup=True, dropFieldIfAllNull=True).json(base_location)

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md The number of rows matches the object counts produced by AWS.

# COMMAND ----------

# expand asset_events array
from pyspark.sql.functions import explode
dfUpdates = df.select(explode(df.asset_events).alias('asset_event')) \
    .select('asset_event.*') \
    .cache()

# COMMAND ----------

nft152_take2 = '/tmp/nft152_20220724'
dfUpdates.write.format('delta').partitionBy('event_type', 'collection_slug').save(nft152_take2)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE '/tmp/nft152_20220724'

# COMMAND ----------

dfUpdates = spark.read.load(nft152_take2)

# COMMAND ----------

dfUpdates.count()
