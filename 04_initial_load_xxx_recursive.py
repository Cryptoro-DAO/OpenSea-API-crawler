# Databricks notebook source
# MAGIC %md
# MAGIC # Initial loading of HAPE Prime and World of Women into `opensea_events` delta table
# MAGIC 
# MAGIC _N.b._ Leaving 0x1A92f7381B9F03921564a437210bB9396471050C in the input directory to test `whenNotMatchedInsertAll`
# MAGIC 
# MAGIC Collection |asset contract address
# MAGIC -|-
# MAGIC HAPE PRIME| 0x4Db1f25D3d98600140dfc18dEb7515Be5Bd293Af
# MAGIC World of Women (WOW) | 0xe785E82358879F061BC3dcAC6f0444462D4b5330

# COMMAND ----------

# MAGIC %fs ls /mnt/opensea-sg/lz/asset_events/asset_contract_address/

# COMMAND ----------

# MAGIC %md # 
# MAGIC 
# MAGIC write a description here

# COMMAND ----------

# MAGIC %ls -v /dbf/mnt/opensea-sg/lz/asset_events/asset_contract_address/0x4Db1f25D3d98600140dfc18dEb7515Be5Bd293Af/ | tail

# COMMAND ----------

base_location = '/mnt/opensea-sg/lz/asset_events/asset_contract_address/'
# df = spark.read.option('recursiveFileLookup', 'true').json(path=base_location, schema=schema)
df = spark.read.option('recursiveFileLookup', 'true').json(path=base_location)
df.write.mode('overwrite') \
    .format('parquet') \
    .save('/tmp/parquet/asset_events')

# COMMAND ----------

df = spark.read.load(format='parquet', path='/tmp/parquet/asset_events').repartition(10)

# COMMAND ----------

df.count()

# COMMAND ----------

display(df.filter('_corrupt_record is not null'))

# COMMAND ----------

df.filter('_corrupt_record is null').count()

# COMMAND ----------

from pyspark.sql.functions import explode
df.select(explode(df.asset_events).alias('asset_event')) \
    .select('asset_event.*') \
    .count()

# COMMAND ----------

from pyspark.sql.functions import explode
df.filter('_corrupt_record is null') \
    .select(explode(df.asset_events).alias('asset_event')) \
    .select('asset_event.*') \
    .write.mode('overwrite') \
    .format('delta') \
    .partitionBy('event_type', 'collection_slug') \
    .save('/tmp/delta/asset_events')

# COMMAND ----------

df = spark.read.load(format='delta', path='/tmp/delta/asset_events')

# COMMAND ----------

type(df)

# COMMAND ----------

df.groupby('collection_slug').count().show()

# COMMAND ----------

# MAGIC %md \* Result above confirms having loaded expected number of records.
# MAGIC The currupted rows might have been due to ZIP files present in the input director.
# MAGIC 
# MAGIC __*To-do:*__ research syntex to exclude files.

# COMMAND ----------

# MAGIC %sql
# MAGIC select collection_slug, count(event_type) from opensea_events group by collection_slug

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY opensea_events

# COMMAND ----------

from delta.tables import *

# spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
# spark.conf.get('spark.databricks.delta.resolveMergeUpdateStructsByName.enabled', 'true')

deltaTblEvents = DeltaTable.forName(spark, 'opensea_events')
# deltaTableEventsUpdate = DeltaTable.forPath(spark, '/tmp/delta/_asset_events_proof-moonbirds')

dfUpdates = df

deltaTblEvents.alias('events') \
  .merge(
    dfUpdates.alias('updates'),
    'events.id = updates.id'
  ) \
  .whenNotMatchedInsertAll() \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select collection_slug, count(event_type) from opensea_events group by collection_slug

# COMMAND ----------

display(deltaTblEvents.history())
