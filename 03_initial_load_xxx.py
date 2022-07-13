# Databricks notebook source
# MAGIC %md
# MAGIC # Initial loading of Clone X and Doodles into `opensea_events` delta table
# MAGIC 
# MAGIC Collection |asset contract address
# MAGIC -|-
# MAGIC Clone X | 0x49cF6f5d44E70224e2E23fDcdd2C053F30aDA28B  
# MAGIC Doodles | 0x8a90CAb2b38dba80c64b7734e58Ee1dB38B8992e

# COMMAND ----------

# MAGIC %fs ls /mnt/opensea/asset_events/asset_contract_address/

# COMMAND ----------

# MAGIC %ls /tmp/asset_contract_address/0x49cF6f5d44E70224e2E23fDcdd2C053F30aDA28B

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /tmp/asset_contract_address/0x49cF6f5d44E70224e2E23fDcdd2C053F30aDA28B | wc -l

# COMMAND ----------

# MAGIC %sh
# MAGIC # unzip CloneX 
# MAGIC unzip -uq /dbfs/mnt/opensea/asset_events/asset_contract_address/0x49cF6f5d44E70224e2E23fDcdd2C053F30aDA28B.zip -d /tmp/asset_contract_address

# COMMAND ----------

# MAGIC %fs mv -r file:/tmp/asset_contract_address /dbfs/mnt/opensea-sg/tmp/asset_contract_address

# COMMAND ----------

# MAGIC %sh mv /dbfs/mnt/opensea/asset_events/asset_contract_address/0x49cF6f5d44E70224e2E23fDcdd2C053F30aDA28B.zip /dbfs/mnt/opensea-sg/imported/asset_events/asset_contract_address/

# COMMAND ----------

# MAGIC %fs ls /dbfs/mnt/opensea-sg/tmp/asset_contract_address

# COMMAND ----------

# MAGIC %sh unzip -l /dbfs/mnt/opensea/asset_events/asset_contract_address/0x49cF6f5d44E70224e2E23fDcdd2C053F30aDA28B.zip | tail

# COMMAND ----------

schema = spark.table(tableName='opensea_events').schema

# COMMAND ----------

base_location = '/mnt/opensea-sg/tmp/asset_contract_address/'
# df = spark.read.option('recursiveFileLookup', 'true').json(path=base_location, schema=schema)
df = spark.read.option('recursiveFileLookup', 'true').json(path=base_location)
df.write.mode('overwrite') \
    .format('parquet') \
    .save('/tmp/parquet/asset_events')

# COMMAND ----------

df = spark.read.load(format='parquet', path='/tmp/parquet/asset_events')

# COMMAND ----------

df.count()

# COMMAND ----------

from pyspark.sql.functions import explode
df.select(explode(df.asset_events).alias('asset_event')) \
    .select('asset_event.*') \
    .count()

# COMMAND ----------

df.select(explode(df.asset_events).alias('asset_event')) \
    .select('asset_event.*') \
    .write.mode('overwrite') \
    .format('delta') \
    .partitionBy('event_type', 'collection_slug') \
    .save('/tmp/delta/asset_events')

# COMMAND ----------

from delta.tables import *

# spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
# spark.conf.get('spark.databricks.delta.resolveMergeUpdateStructsByName.enabled', 'true')

deltaTblEvents = DeltaTable.forName(spark, 'opensea_events')
# deltaTableEventsUpdate = DeltaTable.forPath(spark, '/tmp/delta/_asset_events_proof-moonbirds')

dfUpdates = df.select(explode(df.asset_events).alias('asset_event')) \
    .select('asset_event.*')

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

# COMMAND ----------

# clean up after imported
# %sh rm -r /dbfs/mnt/opensea-sg/tmp/asset_contract_address/0x49cF6f5d44E70224e2E23fDcdd2C053F30aDA28B


# COMMAND ----------



# COMMAND ----------

# %sh
# unzip -uq /dbfs/mnt/opensea/asset_events/asset_contract_address/0x8a90CAb2b38dba80c64b7734e58Ee1dB38B8992e.zip -d /dbfs/tmp/asset_contract_address
# mv -r /tmp/asset_contract_address /dbfs/mnt/opensea-sg/tmp/asset_contract_address
