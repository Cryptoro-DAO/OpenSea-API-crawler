# Databricks notebook source
# MAGIC %md
# MAGIC # Incrementally load `roof-moonbirds` 0x23581767a106ae21c074b2276D25e5C3e136a68b into Delta
# MAGIC 
# MAGIC note: 0x23581767a106ae21c074b2276D25e5C3e136a68b_20220709T113549cst.zip asset_bundle is StringType instead of StructType  
# MAGIC How to fix?

# COMMAND ----------

# MAGIC %sh ls -lhgG /dbfs/mnt/opensea/asset_events/asset_contract_address

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip -u /dbfs/mnt/opensea/asset_events/asset_contract_address/0x23581767a106ae21c074b2276D25e5C3e136a68b.zip -d /tmp/asset_contract_address
# MAGIC unzip -u /dbfs/mnt/opensea/asset_events/asset_contract_address/0x23581767a106ae21c074b2276D25e5C3e136a68b_20220709T0725cst.zip -d /tmp/asset_contract_address

# COMMAND ----------

# MAGIC %sh du -Sh /tmp/asset_contract_address

# COMMAND ----------

# MAGIC %sh ls -vr /tmp/asset_contract_address/0x23581767a106ae21c074b2276D25e5C3e136a68b | head

# COMMAND ----------

# MAGIC %md
# MAGIC move unzipped files to dbfs as a workaround beccuase park.read.json() can't read from local

# COMMAND ----------

# MAGIC %fs mv -r file:/tmp/asset_contract_address/0x23581767a106ae21c074b2276D25e5C3e136a68b dbfs:/tmp/asset_contract_address/0x23581767a106ae21c074b2276D25e5C3e136a68b

# COMMAND ----------

# MAGIC %ls -Ghv /dbfs/tmp/asset_contract_address/0x23581767a106ae21c074b2276D25e5C3e136a68b

# COMMAND ----------

from pyspark.sql.functions import explode

base_location = '/tmp/asset_contract_address/0x23581767a106ae21c074b2276D25e5C3e136a68b/'
df = spark.read.json(base_location)

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Preserve schema, as we would encounter `asset_bundle:string` < this needs to be `struct`

# COMMAND ----------

schema = df.schema

# COMMAND ----------

temp_view = 'asset_events'
df.createOrReplaceTempView(temp_view)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE asset_events

# COMMAND ----------

# more expensive than SQL 
df.describe()

# COMMAND ----------

df.select(explode(df.asset_events).alias('asset_event')) \
    .select('asset_event.*') \
    .count()

# COMMAND ----------

df.select(explode(df.asset_events).alias('asset_event')) \
    .select('asset_event.*') \
    .printSchema()

# COMMAND ----------

df.select(explode(df.asset_events).alias('asset_event')) \
    .select('asset_event.*') \
    .write.mode('overwrite') \
    .format('delta') \
    .partitionBy('event_type', 'collection_slug') \
    .save('/tmp/delta/asset_events_proof-moonbirds')

# COMMAND ----------

# MAGIC %fs ls /tmp/delta

# COMMAND ----------

# MAGIC %ls
# MAGIC mv 

# COMMAND ----------

df = spark.read.format('delta').load('/tmp/delta/asset_events_proof-moonbirds')
df.createOrReplaceTempView('tempView')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(id) from tempView

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE tempView

# COMMAND ----------

# MAGIC %md
# MAGIC archive imported source data

# COMMAND ----------

# MAGIC %fs mv /mnt/opensea/asset_events/asset_contract_address/0x23581767a106ae21c074b2276D25e5C3e136a68b.zip /mnt/opensea-sg/imported/asset_events/asset_contract_address/

# COMMAND ----------

# MAGIC %fs mv /mnt/opensea/asset_events/asset_contract_address/0x23581767a106ae21c074b2276D25e5C3e136a68b_20220709T0725cst.zip /mnt/opensea-sg/imported/asset_events/asset_contract_address/

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incrementally load update

# COMMAND ----------

# MAGIC %sh rm -r /dbfs/tmp/_asset_contract_address/0x23581767a106ae21c074b2276D25e5C3e136a68b

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip -u /dbfs/mnt/opensea/asset_events/asset_contract_address/0x23581767a106ae21c074b2276D25e5C3e136a68b_20220709T113549cst -d /tmp/_asset_contract_address

# COMMAND ----------

# MAGIC %sh ls -v /dbfs/tmp/_asset_contract_address/0x23581767a106ae21c074b2276D25e5C3e136a68b | tail

# COMMAND ----------

import os
from pyspark.sql.functions import explode

# .option('recursiveFileLookup', 'true')
# asset_bundle:string < this needs to but struct
# workaround: explicitly set schema using target's schema

base_location = '/tmp/_asset_contract_address/0x23581767a106ae21c074b2276D25e5C3e136a68b'
df2 = spark.read.format('json').schema(schema).load(base_location)
df2 = df2.select(explode(df2.asset_events).alias('asset_event')) \
     .select('asset_event.*')

# COMMAND ----------

temp_view = 'asset_events_update'
df2.createOrReplaceTempView(temp_view)

# COMMAND ----------

df2['asset_bundle']

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from asset_events_update

# COMMAND ----------

from delta.tables import *

# spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
# spark.conf.get('spark.databricks.delta.resolveMergeUpdateStructsByName.enabled', 'true')

deltaTableEvents = DeltaTable.forPath(spark, '/tmp/delta/asset_events_proof-moonbirds')
# deltaTableEventsUpdate = DeltaTable.forPath(spark, '/tmp/delta/_asset_events_proof-moonbirds')

dfUpdates = df2

deltaTableEvents.alias('events') \
  .merge(
    dfUpdates.alias('updates'),
    'events.id = updates.id'
  ) \
  .whenNotMatchedInsertAll() \
  .execute()

# COMMAND ----------

from pyspark.sql.functions import struct, col
dfUpdates.select("asset_bundle").printSchema()

# COMMAND ----------

deltaTableEvents.toDF().write.mode('overwrite') \
    .format('delta') \
    .partitionBy('event_type', 'collection_slug') \
    .save('/tmp/delta/asset_events_proof-moonbirds_updated')

# COMMAND ----------

# MAGIC %ls -gGh /dbfs/mnt/opensea/asset_events/asset_contract_address

# COMMAND ----------

# MAGIC %ls -gGh /dbfs/mnt/opensea-sg/imported/asset_events/asset_contract_address

# COMMAND ----------

# MAGIC %fs mv /mnt/opensea/asset_events/asset_contract_address/0x23581767a106ae21c074b2276D25e5C3e136a68b_20220709T113549cst.zip /mnt/opensea-sg/imported/asset_events/asset_contract_address/

# COMMAND ----------

# MAGIC %sh du -s /dbfs/tmp/delta/

# COMMAND ----------

# MAGIC %ls -gRGh /dbfs/mnt/opensea-sg/imported/asset_events

# COMMAND ----------

df = spark.read.format('delta').load('/tmp/delta/asset_events_proof-moonbirds')

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY opensea_events

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct collection_slug from opensea_events

# COMMAND ----------

from delta.tables import *

# spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
# spark.conf.get('spark.databricks.delta.resolveMergeUpdateStructsByName.enabled', 'true')

deltaTableEvents = DeltaTable.forName(spark, 'opensea_events')
# deltaTableEventsUpdate = DeltaTable.forPath(spark, '/tmp/delta/_asset_events_proof-moonbirds')

dfUpdates = df

deltaTableEvents.alias('events') \
  .merge(
    dfUpdates.alias('updates'),
    'events.id = updates.id'
  ) \
  .whenNotMatchedInsertAll() \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY opensea_events

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select collection_slug, count(event_type) from opensea_events group by collection_slug
