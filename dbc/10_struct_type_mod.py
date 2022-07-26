# Databricks notebook source
# MAGIC %md # Struct Type column merge issue

# COMMAND ----------

target_table = 'opensea_events'
source_table = '/tmp/nft152'
df_src = spark.table(target_table)
df_tgt = spark.read.load(source_table)

# COMMAND ----------

problem_col = 'dev_fee_payment_event.transaction'

# COMMAND ----------

df_src.select(problem_col).printSchema()

# COMMAND ----------

df_tgt.select(problem_col).printSchema()

# COMMAND ----------

df_src.filter(problem_col + '.from_account is not null').count()

# COMMAND ----------

df_src.filter(problem_col + '.to_account is not null').count()

# COMMAND ----------

import pyspark.sql.functions as f

df_src.filter(problem_col + '.from_account is not null') \
    .select('id', f.posexplode_outer(f.split(f'{problem_col}.from_account', ','))) \
    .withColumn('col', f.regexp_replace('col', "[{} ]", "")) \
    .withColumn('col', f.when(f.col('col') == 'null', None).otherwise(f.col('col'))) \
    .withColumn('col', f.when(f.col('col') == '', None).otherwise(f.col('col'))) \
    .show(20, truncate=False)

# COMMAND ----------

df_src.filter(problem_col + '.from_account is not null').select('id', f.col(f'{problem_col}.from_account').alias('foo')).withColumn('bar', f.expr('substr(foo, 2, length(foo)-2)')).select('id', f.posexplode_outer(f.split('bar', ','))).show(20,truncate=False)

# COMMAND ----------

'{abc}'.strip('{}')

# COMMAND ----------

import pyspark.sql.dataframe as pd
help(pd.DataFrame.withColumns)

# COMMAND ----------


