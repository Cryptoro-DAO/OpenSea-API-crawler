# Databricks notebook source
# MAGIC %md
# MAGIC # Procedure to Load and Update Asset Events

# COMMAND ----------

aws_bucket_name = "opensea-ap-southeast-1"
mount_name = "opensea"
dbutils.fs.updateMount(f"s3a://{aws_bucket_name}", f"/mnt/{mount_name}")
display(dbutils.fs.ls("/mnt/%s" % mount_name))

# COMMAND ----------

# MAGIC %fs ls /mnt/opensea/asset_events/asset_contract_address

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip -uq /dbfs/mnt/opensea/asset_events/asset_contract_address.zip -d /tmp

# COMMAND ----------

# MAGIC %sh du -h /tmp/asset_contract_address

# COMMAND ----------

import os
from pyspark.sql.functions import explode

base_location = 'file:/tmp/asset_contract_address'
path = [_.path[len('file:'):] for _ in dbutils.fs.ls(base_location)]
os.path.basename

# COMMAND ----------

import os
from pyspark.sql.functions import explode

base_location = 'file:/tmp/asset_contract_address'
path = [_.path[len('file:'):-1] for _ in dbutils.fs.ls(base_location)]

for each in path:
    table_name = os.path.basename(each)
    df = spark.read.json(each).cache()
    if '_corrupt_record' in df.columns:
        df2 = df.filter('_corrupt_record is not null')
    else:
        df2 = df
    d2 = df2.select(explode(df.asset_events).alias('asset_event')) \
            .select('asset_event.*')
    display(df2)
#         .select('asset_event.*') \
#         .write.mode('overwrite').format('parquet') \
#         .partitionBy('event_type') \
#         .saveAsTable(table_name)

#     df2 = df.select(explode(df.asset_events).alias('asset_event')) \
#         .select('asset_event.*')
#     df2.write.mode('append').format('delta') \
#         .partitionBy('event_type') \
#         .saveAsTable('asset_contract_events')


# COMMAND ----------

# MAGIC %sql
# MAGIC select collection_slug, event_type, count(id)
# MAGIC from asset_contract_events
# MAGIC group by event_type, collection_slug

# COMMAND ----------

base_location = 'file:/tmp/asset_contract_address'
path = [_.path[len('file:'):] for _ in dbutils.fs.ls(base_location)]
path

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS jsonTable;
# MAGIC 
# MAGIC CREATE TEMPORARY VIEW jsonTable
# MAGIC USING json
# MAGIC OPTIONS (path='/tmp/asset_contract_address/0x60E4d786628Fea6478F785A6d7e704777c86a7c6/');

# COMMAND ----------

from pyspark.sql.functions import explode

df = spark.sql('select * from jsonTable').cache()
df2 = df.select(explode(df.asset_events).alias('asset_event'))

# COMMAND ----------

display(df2.select('asset_event.collection_slug').distinct())

# COMMAND ----------

df2.select('asset_event.*').write.format('delta').mode('overwrite') \
    .partitionBy('event_type', 'collection_slug') \
    .save('/tmp/delta/asset_events_mutant-ape-yacht-club')


# COMMAND ----------

# MAGIC %fs ls /tmp/delta/

# COMMAND ----------

# MAGIC %sh du -h /dbfs/tmp/delta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading and Updating Delta

# COMMAND ----------

from delta.tables import *

deltaTableEvents = DeltaTable.forPath(spark, '/tmp/delta/asset_events_azuki/')
deltaTableEventsUpdates = DeltaTable.forPath(spark, '/tmp/delta/asset_events_boredapeyachtclub')

dfUpdates = deltaTableEventsUpdates.toDF()

deltaTableEvents.alias('events') \
  .merge(
    dfUpdates.alias('updates'),
    'events.id = updates.id'
  ) \
  .whenNotMatchedInsertAll() \
  .execute()

# COMMAND ----------

deltaTableEventsUpdates = DeltaTable.forPath(spark, '/tmp/delta/asset_events_mutant-ape-yacht-club')

dfUpdates = deltaTableEventsUpdates.toDF()

deltaTableEvents.alias('events') \
  .merge(
    dfUpdates.alias('updates'),
    'events.id = updates.id'
  ) \
  .whenNotMatchedInsertAll() \
  .execute()

# COMMAND ----------

# Create a view or table
temp_table_name = "events"
deltaTableEvents.toDF().createOrReplaceTempView(temp_table_name)

# COMMAND ----------

deltaTableEvents.toDF() \
    .write.format('delta').mode('overwrite') \
    .partitionBy('event_type', 'collection_slug') \
.saveAsTable('opensea_events')

# COMMAND ----------

# MAGIC %sql
# MAGIC select collection_slug, count(id) from events group by collection_slug

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from events where event_type = 'collection_offer'

# COMMAND ----------

display(df.filter('event_type = "collection_offer"'))

# COMMAND ----------

# MAGIC %sql
# MAGIC select asset.collection.slug, event_type, count(event_type), max(event_timestamp), min(event_timestamp)
# MAGIC from events
# MAGIC group by event_type, asset.collection.slug

# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks Datasets
# MAGIC 
# MAGIC for learning and experimenting

# COMMAND ----------

# MAGIC %ls /dbfs/databricks-datasets

# COMMAND ----------

# MAGIC %md
# MAGIC # Experiment loading parquet

# COMMAND ----------

# File location and type
# file_location = "/databricks-datasets/amazon/data20K/part-r-00000-112e73de-1ab1-447b-b167-0919dd731adf.gz.parquet"
file_location = "/FileStore/tables/0x6f4a2d3a4f47f9c647d86c929755593911ee91ec.parquet"
file_type = "parquet"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)
df = spark.read.format(file_type) \
    .load(file_location)

display(df)

# COMMAND ----------

# Create a view or table

temp_table_name = "nft20_success_parquet"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC 
# MAGIC select * from `nft20_success_parquet`

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "nft20_success"

df.write.format("parquet").saveAsTable(permanent_table_name)
