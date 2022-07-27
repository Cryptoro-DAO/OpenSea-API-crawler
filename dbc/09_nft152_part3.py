# Databricks notebook source
# MAGIC %md
# MAGIC Incrementally load ... event data retrieved from OpenSea between 2022-07-26 and 2022-07-27.
# MAGIC 
# MAGIC Total number of objects | Total size
# MAGIC -- | --
# MAGIC 17,788 | 841.0 MB

# COMMAND ----------

# MAGIC %fs ls /mnt/opensea-sg/lz/asset_events/20220726

# COMMAND ----------

spark.conf.get('spark.sql.jsonGenerator.ignoreNullFields')

# COMMAND ----------

base_location = '/mnt/opensea-sg/lz/asset_events/20220726'
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

nft152_take3 = '/tmp/nft152_20220726'
dfUpdates.write.format('delta').partitionBy('event_type', 'collection_slug').save(nft152_take3)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE '/tmp/nft152_20220726'

# COMMAND ----------

nft152_take3 = '/tmp/nft152_20220726'
dfUpdates = spark.read.load(nft152_take3)

# COMMAND ----------

dfUpdates.count()
