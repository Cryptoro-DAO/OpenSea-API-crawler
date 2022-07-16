# Databricks notebook source
# MAGIC %md
# MAGIC # Initial loading of Cryptoadz, Meebits and BoredApeKennelClub into `opensea_events` delta table using single node
# MAGIC 
# MAGIC Using a single node, instead of a standard cluster, enables unzip large ZIP file to local drive and process.
# MAGIC This workaround avoids the bottleneck of unzipping to s3.
# MAGIC 
# MAGIC Collection | asset contract address
# MAGIC --- | ---
# MAGIC Cryptoadz (TOADZ) | 0x1CB1A5e65610AEFF2551A50f76a87a7d3fB649C
# MAGIC Meebits | 0x7Bd29408f11D2bFC23c34f18275bBf23bB716Bc7
# MAGIC BoredApeKennelClub (BAKC) | 0xba30E5F9Bb24caa003E9f2f0497Ad287FDF95623

# COMMAND ----------

# MAGIC %fs ls /mnt/opensea-sg/lz/asset_events/asset_contract_address/

# COMMAND ----------

# MAGIC %md # Cryptoadz
# MAGIC 0x1CB1A5e65610AEFF2551A50f76a87a7d3fB649C6

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip -u /dbfs/mnt/opensea-sg/lz/asset_events/asset_contract_address/0x1CB1A5e65610AEFF2551A50f76a87a7d3fB649C6.zip -d /tmp/asset_contract_address/
# MAGIC ls /tmp/asset_contract_address/0x1CB1A5e65610AEFF2551A50f76a87a7d3fB649C6 | wc -l

# COMMAND ----------

# schema = spark.table('opensea_events').schema
# df = spark.read.option('recursiveFileLookup', 'true').json(path=base_location, schema=...)


# COMMAND ----------

base_location = 'file:/tmp/asset_contract_address'
df = spark.read.option('recursiveFileLookup', 'true').json(path=base_location)

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

# expand asset_events array
from pyspark.sql.functions import explode
df_event = df.coalesce(8) \
    .select(explode(df.asset_events).alias('asset_event')) \
    .select('asset_event.*') \
    .cache()

# COMMAND ----------

df_event.count()

# COMMAND ----------

df_event.filter('asset_bundle is not null').count()

# COMMAND ----------

from pyspark.sql.functions import explode

target_location = '/tmp/parquet/asset_events_toadz'

df.select(explode(df.asset_events).alias('asset_event')) \
    .select('asset_event.*') \
    .drop('asset_bundle') \
    .write.mode('overwrite') \
    .format('parquet') \
    .partitionBy('event_type', 'collection_slug') \
    .save(target_location)

# COMMAND ----------

target_location = '/tmp/parquet/asset_events_toadz'
# df = spark.read.load(format='parquet', schema=schema, path='/tmp/parquet/asset_events_toadz')
df_event = spark.read.load(format='parquet', path=target_location)

# COMMAND ----------

df_event.rdd.getNumPartitions()

# COMMAND ----------

df_event.count()

# COMMAND ----------

df_event.groupby('collection_slug').count().show()

# COMMAND ----------

# MAGIC %md ... standying by to merge

# COMMAND ----------

# MAGIC %md # Meebits and BoredApeKennelClub (BAKC)
# MAGIC 
# MAGIC ... with `recursiveFileLookup` 

# COMMAND ----------

# MAGIC %ls /dbfs/mnt/opensea-sg/lz/asset_events/asset_contract_address/0x[7b]*.zip

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip -u /dbfs/mnt/opensea-sg/lz/asset_events/asset_contract_address/0x7Bd29408f11D2bFC23c34f18275bBf23bB716Bc7.zip -d /tmp/asset_contract/
# MAGIC unzip -u /dbfs/mnt/opensea-sg/lz/asset_events/asset_contract_address/0xba30E5F9Bb24caa003E9f2f0497Ad287FDF95623.zip -d /tmp/asset_contract/

# COMMAND ----------

# MAGIC %ls -v /tmp/asset_contract/0x7Bd29408f11D2bFC23c34f18275bBf23bB716Bc7 | tail

# COMMAND ----------

# MAGIC %ls -v /tmp/asset_contract/0xba30E5F9Bb24caa003E9f2f0497Ad287FDF95623 | tail

# COMMAND ----------



# COMMAND ----------

base_location = 'file:/tmp/asset_contract'
df = spark.read.option('recursiveFileLookup', 'true').json(path=base_location)

# COMMAND ----------

df.count()

# COMMAND ----------



# COMMAND ----------

# expand asset_events array
from pyspark.sql.functions import explode
df_event = df.coalesce(8) \
    .select(explode(df.asset_events).alias('asset_event')) \
    .select('asset_event.*') \
    .cache()

# COMMAND ----------

df_event.count()

# COMMAND ----------

df_event.filter(df_event.dev_fee_payment_event.isNotNull()).count()

# COMMAND ----------

from pyspark.sql.functions import explode

target_location = '/tmp/parquet/asset_events_meebits_and_bakc'

df.select(explode(df.asset_events).alias('asset_event')) \
    .select('asset_event.*') \
    .drop('dev_fee_payment_event') \
    .write.mode('overwrite') \
    .format('parquet') \
    .partitionBy('event_type', 'collection_slug') \
    .save(target_location)

# COMMAND ----------

# MAGIC %sql
# MAGIC select collection_slug, count(event_type) from opensea_events group by collection_slug order by count(event_type)

# COMMAND ----------

# MAGIC %md # Merge

# COMMAND ----------

# MAGIC %md ## Toadz

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY opensea_events

# COMMAND ----------

from delta.tables import *

spark.conf.set('spark.databricks.delta.schema.autoMerge.enabled', 'true')
# spark.conf.set('spark.databricks.delta.resolveMergeUpdateStructsByName.enabled', 'true')

deltaTblEvents = DeltaTable.forName(spark, 'opensea_events')

path = '/tmp/parquet/asset_events_toadz'
dfUpdates = spark.read.parquet(pathS)

deltaTblEvents.alias('events') \
  .merge(
    dfUpdates.alias('updates'),
    'events.id = updates.id') \
  .whenNotMatchedInsertAll() \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select collection_slug, count(event_type) from opensea_events group by collection_slug order by count(event_type)

# COMMAND ----------

# MAGIC %md ## Meebits and BAKC

# COMMAND ----------

path = '/tmp/parquet/asset_events_meebits_and_bakc'
dfUpdates = spark.read.parquet(path)

deltaTblEvents.alias('events') \
  .merge(
    dfUpdates.alias('updates'),
    'events.id = updates.id') \
  .whenNotMatchedInsertAll() \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select collection_slug, count(event_type) from opensea_events group by collection_slug order by count(event_type)

# COMMAND ----------

display(deltaTblEvents.history())
