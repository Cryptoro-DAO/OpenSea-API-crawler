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

# MAGIC %fs ls dbfs:/tmp/asset_contract_address/0x23581767a106ae21c074b2276D25e5C3e136a68b

# COMMAND ----------

# MAGIC %fs ls file:/tmp/asset_contract_address/0x23581767a106ae21c074b2276D25e5C3e136a68b

# COMMAND ----------

from pyspark.sql.functions import explode

base_location = '/tmp/asset_contract_address/0x23581767a106ae21c074b2276D25e5C3e136a68b/'
df = spark.read.json(base_location)

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
    .printSchema()

# COMMAND ----------

df.select(explode(df.asset_events).alias('asset_event')) \
    .select('asset_event.*') \
    .write.mode('overwrite') \
    .format('delta') \
    .partitionBy('event_type', 'collection_slug') \
    .save('/tmp/delta/asset_events_proof-moonbirds')

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

# MAGIC %fs ls /tmp/delta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incrementally load update

# COMMAND ----------

# MAGIC %sh rm -r /dbfs/tmp/_asset_contract_address/0x23581767a106ae21c074b2276D25e5C3e136a68b

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip -u /dbfs/mnt/opensea/asset_events/asset_contract_address/0x23581767a106ae21c074b2276D25e5C3e136a68b_20220709T113549cst -d /dbfs/tmp/_asset_contract_address

# COMMAND ----------

# MAGIC %sh ls -v /dbfs/tmp/_asset_contract_address/0x23581767a106ae21c074b2276D25e5C3e136a68b | tail

# COMMAND ----------

import os
from pyspark.sql.functions import explode

base_location = '/tmp/_asset_contract_address/0x23581767a106ae21c074b2276D25e5C3e136a68b/'
df2 = spark.read.json(base_location)
df2 = df2.select(explode(df2.asset_events).alias('asset_event')) \
    .select('asset_event.*')

# COMMAND ----------

# MAGIC %md
# MAGIC ## asset_bundle:string < this needs to but struct

# COMMAND ----------

temp_view = 'asset_events_update'
df2.createOrReplaceTempView(temp_view)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from asset_events_update

# COMMAND ----------

deltaTableEventsUpdates = DeltaTable.forPath(spark, '/tmp/delta/asset_events_proof-moonbirds')
type(deltaTableEventsUpdates.toDF())

# COMMAND ----------

spark.conf.get("spark.databricks.delta.schema.autoMerge.enabled")

# COMMAND ----------

spark.conf.get('spark.databricks.delta.resolveMergeUpdateStructsByName.enabled')

# COMMAND ----------

from delta.tables import *

# spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
# spark.conf.get('spark.databricks.delta.resolveMergeUpdateStructsByName.enabled', 'true')

deltaTableEvents = DeltaTable.forPath(spark, '/tmp/delta/asset_events_proof-moonbirds')
deltaTableEventsUpdate = DeltaTable.forPath(spark, '/tmp/delta/_asset_events_proof-moonbirds')

dfUpdates = deltaTableEventsUpdate.toDF()

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

type(dfUpdates["asset_bundle"])

# COMMAND ----------

deltaTableEvents.toDF().select('asset_bundle').printSchema()

# COMMAND ----------

df2 = dfUpdates.select(col("asset_bundle").cast(transform_schema(df.schema)['hid_tagged'].dataType))

# COMMAND ----------

# MAGIC %fs ls /mnt/opensea/asset_events

# COMMAND ----------

# MAGIC %fs mkdirs /mnt/opensea-sg/imported/asset_events/asset_contract_address

# COMMAND ----------

# MAGIC %fs ls /mnt/opensea-sg/imported/asset_events/asset_contract_address

# COMMAND ----------

# MAGIC %fs ls /mnt/opensea/asset_events/asset_contract_address

# COMMAND ----------

# MAGIC %fs mv /mnt/opensea/asset_events/asset_contract_address/0x23581767a106ae21c074b2276D25e5C3e136a68b_20220709T0725cst.zip /mnt/opensea-sg/imported/asset_events/asset_contract_address/

# COMMAND ----------

# MAGIC %fs mv /mnt/opensea/asset_events/asset_contract_address.zip /mnt/opensea-sg/imported/asset_events/

# COMMAND ----------

# MAGIC %ls /tmp/asset_contract_address/0x23581767a106ae21c074b2276D25e5C3e136a68b

# COMMAND ----------

# MAGIC %ls -ls /tmp/asset_contract_address/0x23581767a106ae21c074b2276D25e5C3e136a68b

# COMMAND ----------


