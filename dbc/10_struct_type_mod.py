# Databricks notebook source
# MAGIC %md # Struct Type column merge issue
# MAGIC 
# MAGIC - Failed to merge fields 'dev_fee_payment_event' and 'dev_fee_payment_event'.
# MAGIC - Failed to merge fields 'transaction' and 'transaction'.
# MAGIC - Failed to merge fields 'from_account' and 'from_account'.
# MAGIC - Failed to merge incompatible data types StringType and StructType(StructField(address,StringType,true),StructField(config,StringType,true),StructField(profile_img_url,StringType,true),StructField(user,StringType,true))

# COMMAND ----------

# MAGIC %fs ls /tmp

# COMMAND ----------

target_table = 'opensea_events'
tgt_df = spark.table(target_table)
source_table = '/tmp/nft152_20220726'
src_df = spark.read.load(source_table)

# COMMAND ----------

problem_col = 'dev_fee_payment_event'
problem_fld = 'transaction'

# COMMAND ----------

tgt_df.select(f'{problem_col}.{problem_fld}').printSchema()

# COMMAND ----------

src_df.select(f'{problem_col}.{problem_fld}').printSchema()

# COMMAND ----------

problem_fld_1 = 'transaction.from_account'
tgt_df.filter(f'{problem_col}.{problem_fld_1} is not null').count()

# COMMAND ----------

tgt_df.filter(f'{problem_col}.{problem_fld_1} is not null').select('id', f'{problem_col}.{problem_fld_1}').show(truncate=False)

# COMMAND ----------

problem_fld_2 = 'transaction.to_account'
tgt_df.filter(f'{problem_col}.{problem_fld_2} is not null').count()

# COMMAND ----------

tgt_df.filter(f'{problem_col}.{problem_fld_2} is not null').select('id', f'{problem_col}.{problem_fld_2}').show(truncate=False)

# COMMAND ----------

# MAGIC %md Comparing to the source

# COMMAND ----------

src_df.filter(f'{problem_col}.{problem_fld_1} is not null').count()

# COMMAND ----------

src_df.filter(f'{problem_col}.{problem_fld_1} is not null').select('id', f'{problem_col}.{problem_fld_1}.*').show(truncate=False)

# COMMAND ----------

src_df.filter(f'{problem_col}.{problem_fld_2} is not null').count()

# COMMAND ----------

# MAGIC %md Copy the problem field, delete the orginal, and update the table

# COMMAND ----------

import pyspark.sql.functions as f

problem_fld_1 = 'transaction.from_account'
renamed_fld_1 = 'transaction.bad_from_account'
problem_fld_2 = 'transaction.to_account'
renamed_fld_2 = 'transaction.bad_to_account'

tgt_df.withColumn(f'{problem_col}', f.col(problem_col).withField(renamed_fld_1, f.col(f'{problem_col}.{problem_fld_1}')) \
                                                      .withField(renamed_fld_2, f.col(f'{problem_col}.{problem_fld_2}')) \
                                                      .dropFields(problem_fld_1, problem_fld_2)) \
    .write \
    .option('overwriteSchema', 'true') \
    .saveAsTable(target_table, format='delta', mode='overwrite', partitionBy=['event_type', 'collection_slug'])

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC describe history opensea_events

# COMMAND ----------



# COMMAND ----------

tgt_df = spark.table(target_table)

# COMMAND ----------

problem_col = 'transaction'
problem_fld = 'to_account.user'

# COMMAND ----------

tgt_df.select(problem_col).printSchema()

# COMMAND ----------

tgt_df.filter(f'{problem_col}.{problem_fld} is not null').count()

# COMMAND ----------

tgt_df.filter(f'{problem_col}.{problem_fld} is not null').select('id', f'{problem_col}.{problem_fld}').show(truncate=False)

# COMMAND ----------

src_df.select(problem_col).printSchema()

# COMMAND ----------

src_df.filter(f'{problem_col}.{problem_fld} is not null').select('id', f'{problem_col}.{problem_fld}.*').show(truncate=False)

# COMMAND ----------

# MAGIC %md # from_account

# COMMAND ----------



# COMMAND ----------

tgt_df = spark.table(target_table)

# COMMAND ----------

problem_col = 'from_account'
problem_fld = 'to_account.user'

# COMMAND ----------

tgt_df.select(problem_col).printSchema()

# COMMAND ----------

tgt_df.filter(f'{problem_col}.{problem_fld} is not null').count()

# COMMAND ----------

tgt_df.filter(f'{problem_col}.{problem_fld} is not null').select('id', f'{problem_col}.{problem_fld}').show(truncate=False)

# COMMAND ----------

src_df.select(problem_col).printSchema()

# COMMAND ----------

src_df.filter(f'{problem_col}.{problem_fld} is not null').select('id', f'{problem_col}.{problem_fld}.*').show(truncate=False)

# COMMAND ----------

# MAGIC %md ## StructType

# COMMAND ----------

from pyspark.sql.types import StructType, StructField
StructType([])

# COMMAND ----------

import pyspark.sql.functions as f

tgt_df.filter(f'{problem_col}.{problem_fld_1} is not null') \
    .select('id', f.posexplode_outer(f.split(f'{problem_col}.{problem_fld_1}', ','))) \
    .withColumn('col', f.regexp_replace('col', "[{} ]", "")) \
    .withColumn('col', f.when(f.col('col') == 'null', None).otherwise(f.col('col'))) \
    .withColumn('col', f.when(f.col('col') == '', None).otherwise(f.col('col'))) \
    .show(20, truncate=False)

# COMMAND ----------

df_src.filter(problem_col + '.from_account is not null').select('id', f.col(f'{problem_col}.from_account').alias('foo')).withColumn('bar', f.expr('substr(foo, 2, length(foo)-2)')).select('id', f.posexplode_outer(f.split('bar', ','))).show(20,truncate=False)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md Code block to generate sample data for Shu Wu
# MAGIC ```python
# MAGIC target_table = 's3://sandbox-sg/dmz/target_sample'
# MAGIC source_table = 's3://sandbox-sg/dmz/source_sample'
# MAGIC save_mode = 'overwrite'
# MAGIC spark.conf.get('spark.sql.sources.default')
# MAGIC 
# MAGIC df_src.filter(problem_col + '.to_account is not null').select('id', 'dev_fee_payment_event').write.save(path=source_table, mode=save_mode)
# MAGIC 
# MAGIC df_tgt.filter(problem_col + '.to_account is not null').select('id', 'dev_fee_payment_event').write.save(path=target_table, mode=save_mode)
# MAGIC ```
