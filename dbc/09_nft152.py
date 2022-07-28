# Databricks notebook source
# MAGIC %md
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

src_path = [f'/mnt/opensea-sg/lz/asset_events/202207{ea}' for ea in ['19', '24', '26']]

# COMMAND ----------

spark.conf.get('spark.sql.jsonGenerator.ignoreNullFields')

# COMMAND ----------

df = spark.read.options(recursiveFileLookup=True, dropFieldIfAllNull=True).json(src_path)

# COMMAND ----------

df.count()

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

# MAGIC %sql
# MAGIC OPTIMIZE '/tmp/nft152'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY 'tmp/nft152'

# COMMAND ----------

df_events = spark.read.load(save_path)
df_events.count() / 1e6

# COMMAND ----------

df_events.printSchema()

# COMMAND ----------


