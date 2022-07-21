# Databricks notebook source
# MAGIC %md # Loading `successful` events on 21 NFTs
# MAGIC 
# MAGIC 21 NFT `successful` events extracted on 2022-06-28T17:51

# COMMAND ----------

# MAGIC %fs ls /mnt/opensea-sg/lz/asset_events/successful/asset_contract_address/

# COMMAND ----------

# MAGIC %md 2.8 GB, 4205 JSON, each JSON consists ~100 events

# COMMAND ----------

base_location = '/mnt/opensea-sg/lz/asset_events/successful/asset_contract_address/'
df = spark.read.option('recursiveFileLookup', 'true').json(path=base_location).coalesce(32)

# COMMAND ----------

df.count()

# COMMAND ----------

# expand asset_events array
from pyspark.sql.functions import explode
dfUpdates = df.select(explode(df.asset_events).alias('asset_event')) \
    .select('asset_event.*') \
    .cache()

# COMMAND ----------

display(dfUpdates.groupby('collection_slug', 'event_type').count())

# COMMAND ----------

dfUpdates.count()

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

display(deltaTblEvents.toDF().filter('event_type = "successful"').groupby('collection_slug').count())

# COMMAND ----------

display(deltaTblEvents.toDF().groupby('collection_slug').count())

# COMMAND ----------

display(deltaTblEvents.history())

# COMMAND ----------


