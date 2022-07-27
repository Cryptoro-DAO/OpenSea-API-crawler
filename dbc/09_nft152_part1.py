# Databricks notebook source
# MAGIC %md
# MAGIC Incrementally load ... event data retrieved from OpenSea between 2022-07-19 and 2022-07-24.
# MAGIC 
# MAGIC Total number of objects | Total size
# MAGIC -- | --
# MAGIC 384,450 | 27.4 GB

# COMMAND ----------

# MAGIC %fs ls /mnt/opensea-sg/lz/asset_events/20220724

# COMMAND ----------

# MAGIC %fs ls /mnt/opensea-sg/lz/asset_events/20220719/asset_contract_address

# COMMAND ----------

base_location = '/mnt/opensea-sg/lz/asset_events/20220719'
df = spark.read.option('recursiveFileLookup', 'true').json(base_location)

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md The number of rows matches the object counts produced by AWS.

# COMMAND ----------

# expand asset_events array
from pyspark.sql.functions import explode
dfUpdates = df.select(explode(df.asset_events).alias('asset_event')) \
    .select('asset_event.*') \
    .cache()

# COMMAND ----------

dfUpdates.count()

# COMMAND ----------

dfUpdates.to_pandas_on_spark() \
    .groupby('collection_slug') \
    .agg({'event_type': 'count', 'event_timestamp': ['min', 'max']})

# COMMAND ----------

dfUpdates.write.format('delta').partitionBy('event_type', 'collection_slug').save('/tmp/nft152')

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE '/tmp/nft152'

# COMMAND ----------

# MAGIC %md
# MAGIC # Schema merging issue
# MAGIC ```
# MAGIC Failed to merge fields 'dev_fee_payment_event' and 'dev_fee_payment_event'.
# MAGIC Failed to merge fields 'transaction' and 'transaction'.
# MAGIC Failed to merge fields 'from_account' and 'from_account'.
# MAGIC Failed to merge incompatible data types StringType and StructType(StructField(address,StringType,true),StructField(config,StringType,true),StructField(profile_img_url,StringType,true),StructField(user,StringType,true))
# MAGIC ```

# COMMAND ----------

dt = spark.table('opensea_events')

# COMMAND ----------

dt.count() / 1e6

# COMMAND ----------

dfUpdates = spark.read.load(path='/tmp/nft152')

# COMMAND ----------

dfUpdates.count() / 1e6

# COMMAND ----------

dt.select("dev_fee_payment_event.transaction.to_account").printSchema()

# COMMAND ----------

dfUpdates.select("dev_fee_payment_event.transaction.to_account").printSchema()

# COMMAND ----------

import pyspark.sql.functions as f
dt.schema['dev_fee_payment_event']

# COMMAND ----------

# It's not possible to rename a struct field (within a column of struct type)
dt2=dt.withColumnRenamed('dev_fee_payment_event.asset', 'dev_fee_payment_event.noway')

# COMMAND ----------

from pyspark.sql.functions import col
dt2 = dt.withColumn('dev_fee_payment_event_copy', col('dev_fee_payment_event'))

# COMMAND ----------

dt2.write.saveAsTable('nft152_tmp', format='delta', partionBy=['asset_event', 'collection_slug'])

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE opensea_events

# COMMAND ----------

# MAGIC %sql
# MAGIC select transaction.from_account.* from 
# MAGIC   (select dev_fee_payment_event.* from nft152_tmp where dev_fee_payment_event.transaction.to_account is not null)

# COMMAND ----------

dfUpdates.createOrReplaceTempView('nft_update')

# COMMAND ----------

# MAGIC %sql
# MAGIC select dev_fee_payment_event.transaction.* from nft_update
# MAGIC   where dev_fee_payment_event.transaction.to_account is not null

# COMMAND ----------

dt2.write.save(path='/tmp/nft152_renamed_column', format='delta', partionBy=['asset_event', 'collection_slug'])

# COMMAND ----------

dt2 = spark.read.load(path='/tmp/nft152_renamed_column')

# COMMAND ----------

dt.printSchema()

# COMMAND ----------



# COMMAND ----------

(dt.select("transaction").schema.

# COMMAND ----------

dt.select("transaction").printSchema()

# COMMAND ----------

dfUpdates.select("transaction").printSchema()

# COMMAND ----------

dt.select("from_account").printSchema()

# COMMAND ----------

dfUpdates.select("from_account").printSchema()

# COMMAND ----------

def schema_diff(schema1, schema2):

    return {
        'fields_in_1_not_2': set(schema1) - set(schema2),
        'fields_in_2_not_1': set(schema2) - set(schema1)
    }

# COMMAND ----------

(schema_diff(dt.select("transaction").schema, dfUpdates.select("transaction").schema))

# COMMAND ----------

dfUpdates.createOrReplaceTempView('nft152')

# COMMAND ----------

# MAGIC %sql select dev_fee_payment_event.transaction.from_account from nft152 where dev_fee_payment_event.transaction.from_account is not null limit 20

# COMMAND ----------

from pyspark.sql.functions import col
# display(df.filter("dev_fee_payment_event.transaction.from_account is not null").select("dev_fee_payment_event.transaction.from_account"))
type(df.filter("dev_fee_payment_event.transaction.from_account is not null").select(col("dev_fee_payment_event.transaction.from_account")))

# COMMAND ----------

df.select("dev_fee_payment_event.transaction.*").filter("dev_fee_payment_event.transaction.from_account")).show()

# COMMAND ----------

dfUpdates.select("dev_fee_payment_event.transaction.from_account")

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

display(dfUpdates.filter("dev_fee_payment_event.transaction.from_account is not null").select("dev_fee_payment_event.transaction.from_account"))

# COMMAND ----------

# MAGIC %md # Merge

# COMMAND ----------

df = spark.table('opensea_events')

# COMMAND ----------

display(df.filter(df.dev_fee_payment_event['transaction']['from_account'].isNotNull()).select(df.dev_fee_payment_event['transaction']['from_account']))

# COMMAND ----------



# COMMAND ----------

display(df.filter('dev_fee_payment_event is not null').select('dev_fee_payment_event'))

# COMMAND ----------

spark.conf.get('spark.databricks.delta.schema.autoMerge.enabled')

# COMMAND ----------

from delta.tables import *

spark.conf.set('spark.databricks.delta.schema.autoMerge.enabled', 'true')
# spark.conf.set('spark.databricks.delta.resolveMergeUpdateStructsByName.enabled', 'true')

deltaTblEvents = DeltaTable.forName(spark, 'opensea_events')

deltaTblEvents.alias('events') \
  .merge(dfUpdates.alias('updates'), 'events.id = updates.id') \
  .whenNotMatchedInsertAll() \
  .execute()

# COMMAND ----------

display(deltaTblEvents.history())

# COMMAND ----------

# MAGIC %sql select collection_slug, event_type, count(event_type) from opensea_events group by event_type, collection_slug order by collection_slug

# COMMAND ----------

# MAGIC %md # Pandas on Spark

# COMMAND ----------

psdf = dfUpdates.to_pandas_on_spark()

# COMMAND ----------



# COMMAND ----------

psdf.groupby('collection_slug').agg({'event_type': 'count', 'event_timestamp': ['min', 'max']})

# COMMAND ----------

psdf_opensea = spark.table('opensea_events').to_pandas_on_spark()
psdf_opensea.groupby('collection_slug').agg({'event_type': 'count', 'event_timestamp': ['min', 'max']})

# COMMAND ----------


