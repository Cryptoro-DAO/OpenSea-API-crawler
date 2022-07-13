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

# MAGIC %fs mv -r /tmp/asset_contract_address /dbfs/mnt/opensea-sg/tmp/asset_contract_address

# COMMAND ----------

# MAGIC %sh mv /dbfs/mnt/opensea/asset_events/asset_contract_address/0x49cF6f5d44E70224e2E23fDcdd2C053F30aDA28B.zip /dbfs/mnt/opensea-sg/imported/asset_events/asset_contract_address/

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip -uq /dbfs/mnt/opensea/asset_events/asset_contract_address/0x49cF6f5d44E70224e2E23fDcdd2C053F30aDA28B.zip 0x49cF6f5d44E70224e2E23fDcdd2C053F30aDA28B/1.json -d /tmp/foo
# MAGIC ls /tmp/foo/0x49cF6f5d44E70224e2E23fDcdd2C053F30aDA28B

# COMMAND ----------

# MAGIC %sh unzip -l /dbfs/mnt/opensea/asset_events/asset_contract_address/0x49cF6f5d44E70224e2E23fDcdd2C053F30aDA28B.zip | tail

# COMMAND ----------

# clean up after imported
# %sh rm -r /dbfs/mnt/opensea-sg/tmp/asset_contract_address/0x49cF6f5d44E70224e2E23fDcdd2C053F30aDA28B


# COMMAND ----------

schema = spark.table(tableName='opensea_events').schema

# COMMAND ----------

base_location = '/mnt/opensea-sg/tmp/asset_contract_address/'
df = spark.read.option('recursiveFileLookup', 'true').json(path=base_location, schema=schema)
df.write.mode('overwrite') \
    .format('parquet') \
    .partitionBy('event_type', 'collection_slug') \
    .save('/tmp/parquet/asset_events')

# COMMAND ----------

df = spark.read.load(format='parquet', path='/tmp/parquet/asset_events', schema=schema)

# COMMAND ----------

df.count()

# COMMAND ----------

df.select(explode(df.asset_events).alias('asset_event')) \
    .select('asset_event.*') \
    .count()

# COMMAND ----------

# %sh
# unzip -uq /dbfs/mnt/opensea/asset_events/asset_contract_address/0x8a90CAb2b38dba80c64b7734e58Ee1dB38B8992e.zip -d /dbfs/tmp/asset_contract_address
# mv -r /tmp/asset_contract_address /dbfs/mnt/opensea-sg/tmp/asset_contract_address
