# Databricks notebook source
# MAGIC %md # Initial load of KaijuKingz, Cool Pets and Lazy Lions
# MAGIC 
# MAGIC ... with `recursiveFileLookup` 
# MAGIC 
# MAGIC Collection | asset contract address | Total size | Total number of objects
# MAGIC --- | --- | --- | ---
# MAGIC KaijuKingz (KAIJU) | 0x0c2E57EFddbA8c768147D1fdF9176a0A6EBd5d83 | 1.6 GB | 60,317
# MAGIC Cool Pets (PETS) | 0x86C10D10ECa1Fca9DAF87a279ABCcabe0063F247 | 1.2 GB | 55,759
# MAGIC Lazy Lions (LION) | 0x8943C7bAC1914C9A7ABa750Bf2B6B09Fd21037E0 | 1.5 GB | 55,807

# COMMAND ----------

# MAGIC %fs ls /mnt/opensea-sg/lz/asset_events/asset_contract_address/20220718

# COMMAND ----------

# MAGIC %md Gzip JSON, 100 events per JSON. _N.b._ the last file retrieved might contain less than 100 events
# MAGIC Total number of objects|Total size
# MAGIC ---:|---:
# MAGIC 171,983|4.2 GB

# COMMAND ----------

base_location = '/mnt/opensea-sg/lz/asset_events/asset_contract_address/20220718'
df = spark.read.option('recursiveFileLookup', 'true').json(path=base_location).coalesce(128)

# COMMAND ----------

df.count()

# COMMAND ----------

# expand asset_events array
from pyspark.sql.functions import explode
dfUpdates = df.select(explode(df.asset_events).alias('asset_event')) \
    .select('asset_event.*') \
    .cache()

# COMMAND ----------

dfUpdates.groupby('collection_slug', 'asset.asset_contract.address').count().show()

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

display(deltaTblEvents.history())

# COMMAND ----------

# MAGIC %sql select collection_slug, event_type, count(event_type) from opensea_events group by event_type, collection_slug order by collection_slug

# COMMAND ----------

# MAGIC %sql select count(event_type) from opensea_events
