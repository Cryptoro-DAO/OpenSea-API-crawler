# Databricks notebook source
# MAGIC %md # NFT152
# MAGIC Initial load of event data of 152 NFT collections, i.e. nft152, retrieved from OpenSea.
# MAGIC 
# MAGIC Part # | Retrieved From | Total number of objects | Total size
# MAGIC -- | -- | -- | --
# MAGIC 1 | 2022-07-19 ~ 24 | 384,450 | 27.4 GB
# MAGIC 2 | 2022-07-24 ~ 26 | 195,356 | 14.8 GB
# MAGIC 3 | 2022-07-26 ~ 27 | 17,788  | 841.0 MB

# COMMAND ----------

# MAGIC %sh du -h /dbfs/mnt/opensea-sg/lz/asset_events/20220719

# COMMAND ----------

# MAGIC %sh du -h /dbfs/mnt/opensea-sg/lz/asset_events/20220724

# COMMAND ----------

# MAGIC %sh du -h /dbfs/mnt/opensea-sg/lz/asset_events/20220726

# COMMAND ----------

# MAGIC %md ## Load JSON and convert to Delta

# COMMAND ----------

src_path = [f'/mnt/opensea-sg/lz/asset_events/202207{ea}' for ea in ['19', '24', '26']]

# COMMAND ----------

spark.conf.get('spark.sql.jsonGenerator.ignoreNullFields')

# COMMAND ----------

df = spark.read.options(recursiveFileLookup=True, dropFieldIfAllNull=True).json(src_path)

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md The count matches AWS.

# COMMAND ----------

# expand asset_events array
from pyspark.sql.functions import explode
df_events = df.select(explode(df.asset_events).alias('asset_event')) \
    .select('asset_event.*') \
    .cache()

# COMMAND ----------

spark.conf.get('spark.sql.sources.default')

# COMMAND ----------

save_path = '/tmp/nft152'
df_events.write.save(save_path, format='delta', mode='overwrite', partitionBy=['event_type', 'collection_slug'])

# COMMAND ----------

# MAGIC %md ## Optimize Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE '/tmp/nft152'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY '/tmp/nft152'

# COMMAND ----------

ops_metics = _sqldf

# COMMAND ----------

import pyspark.sql.functions as f
_sqldf.select(f.col('operationMetrics').getItem('numRemovedFiles').cast('int'))

# COMMAND ----------

psdf = _sqldf.to_pandas_on_spark()
pdf = _sqldf.toPandas()

# COMMAND ----------

ops = pdf.from_dict([ea for ea in pdf['operationMetrics']])

# COMMAND ----------

ops_ = ops.fillna(0).astype('int').sum()

# COMMAND ----------

ops_.index

# COMMAND ----------

ops_.numFiles + ops_['numAddedFiles'] - ops_['numRemovedFiles']

# COMMAND ----------

df_events.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md __Optimization result__: Ran optimize twice. Started with 405,619 files. Ended with 1,350 files and 477 partitions.

# COMMAND ----------

# MAGIC %md # Dataset summary

# COMMAND ----------

save_path = '/tmp/nft152'
df_events = spark.read.load(save_path)
df_events.count() / 1e6

# COMMAND ----------

df_events.printSchema()

# COMMAND ----------

import pyspark.sql.functions as f
df_events.groupby('collection_slug').count().orderBy(f.col('count').desc()).display()

# COMMAND ----------

# MAGIC %sql select count(distinct collection_slug) from delta.`/tmp/nft152`

# COMMAND ----------

# MAGIC %sql
# MAGIC select collection_slug, count(event_type), max(event_timestamp), min(event_timestamp)
# MAGIC   from delta.`/tmp/nft152`
# MAGIC   group by collection_slug

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view collection_slug_stat
# MAGIC   as select collection_slug, count(id) as num_event, max(event_timestamp) as max_timestamp, min(event_timestamp) as min_timestamp
# MAGIC     from delta.`/tmp/nft152`
# MAGIC     group by collection_slug

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from collection_slug_stat
