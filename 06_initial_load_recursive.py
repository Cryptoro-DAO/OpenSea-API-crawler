# Databricks notebook source
# MAGIC %md
# MAGIC # Initial loading of 6 Collections into `opensea_events` delta table
# MAGIC 
# MAGIC ... with `recursiveFileLookup` 
# MAGIC 
# MAGIC Collection | asset contract address
# MAGIC --- | ---
# MAGIC || 0x59468516a8259058baD1cA5F8f4BFF190d30E066
# MAGIC | | 0x67D9417C9C3c250f61A83C7e8658daC487B56B09
# MAGIC | | 0x79FCDEF22feeD20eDDacbB2587640e45491b757f
# MAGIC || 0x7EA3Cca10668B8346aeC0bf1844A49e995527c8B
# MAGIC || 0x9A534628B4062E123cE7Ee2222ec20B86e16Ca8F
# MAGIC || 0xd2F668a8461D6761115dAF8Aeb3cDf5F40C532C6

# COMMAND ----------

# MAGIC %fs ls /mnt/opensea-sg/lz/asset_events/asset_contract_address

# COMMAND ----------

base_location = '/mnt/opensea-sg/lz/asset_events/asset_contract_address'
dbutils.fs.ls(base_location)[0].isDir()

# COMMAND ----------

base_location = '/mnt/opensea-sg/lz/asset_events/asset_contract_address'
df = spark.read.option('recursiveFileLookup', 'true').json(path=base_location).coalesce(32)

# COMMAND ----------

df.count()

# COMMAND ----------



# COMMAND ----------

# expand asset_events array
from pyspark.sql.functions import explode
dfUpdates = df.select(explode(df.asset_events).alias('asset_event')) \
    .select('asset_event.*') \
    .cache()

# COMMAND ----------

dfUpdates.groupby('collection_slug', 'asset.asset_contract.address').count().show()

# COMMAND ----------

# MAGIC %md # Merge

# COMMAND ----------

from delta.tables import *

# spark.conf.set('spark.databricks.delta.schema.autoMerge.enabled', 'true')
# spark.conf.set('spark.databricks.delta.resolveMergeUpdateStructsByName.enabled', 'true')

deltaTblEvents = DeltaTable.forName(spark, 'opensea_events')

deltaTblEvents.alias('events') \
  .merge(dfUpdates.alias('updates'),
    'events.id = updates.id') \
  .whenNotMatchedInsertAll() \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select collection_slug, event_type, count(event_type) from opensea_events group by collection_slug, event_type order by count(event_type)

# COMMAND ----------

display(deltaTblEvents.history())

# COMMAND ----------

# MAGIC %md # Summary Stats

# COMMAND ----------

deltaTblEvents.toDF().rdd.getNumPartitions()

# COMMAND ----------


