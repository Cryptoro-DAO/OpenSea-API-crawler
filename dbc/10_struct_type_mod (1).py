# Databricks notebook source
# MAGIC %md # Struct Type column merge issue

# COMMAND ----------

# MAGIC %md ## Sample data

# COMMAND ----------

source_table = 's3://sandbox-sg/dmz/source_sample'
target_table = 's3://sandbox-sg/dmz/target_sample'
df_src = spark.read.load(source_table)
df_tgt = spark.read.load(target_table)

# COMMAND ----------

problem_col = 'dev_fee_payment_event.transaction'

# COMMAND ----------

df_src.select(problem_col).printSchema()

# COMMAND ----------

# MAGIC %md The schema actually needs to look like below...

# COMMAND ----------

df_tgt.select(problem_col).printSchema()

# COMMAND ----------

df_src.filter(problem_col + '.from_account is not null').count()

# COMMAND ----------

df_src.filter(problem_col + '.to_account is not null').count()

# COMMAND ----------

# MAGIC %md Thankfully, we haven't gotten too many bad data to deal with... so far.

# COMMAND ----------

# MAGIC %md ## Coding challenge 1

# COMMAND ----------

# MAGIC %md ... and meanwhile, I attempted to use `withColumns(*colMap)` together `pyspark.sql.functions.posexplode_outer` but failed.
# MAGIC 
# MAGIC 
# MAGIC Insead `select('*', ...)` is the best workaround thus far.
# MAGIC 
# MAGIC I still like to know how `withColumns` works. It might come in handy someday.

# COMMAND ----------

# MAGIC %md First, the workaround...

# COMMAND ----------

import pyspark.sql.functions as func

display(
df_src.filter(problem_col + '.from_account is not null') \
    .select('id', func.col(f'{problem_col}.from_account').alias('foo')) \
    .select('*', f.posexplode_outer(f.split('foo', ',')))
)

# COMMAND ----------

# MAGIC %md Then, below are the failed attempts...

# COMMAND ----------

df_src.filter(problem_col + '.from_account is not null') \
    .select('id', func.col(f'{problem_col}.from_account').alias('foo')) \
    .withColumn('bar', func.posexplode_outer(func.split('foo', ',')))

# COMMAND ----------

# MAGIC %md ... above failed because two aliases, i.e. `pos` and `col` are exexcted

# COMMAND ----------

df_src.filter(problem_col + '.from_account is not null') \
    .select('id', func.col(f'{problem_col}.from_account').alias('foo')) \
    .withColumns(colsMap={'p': 'pos', 'c': 'col'} ,func.posexplode_outer(f.split('foo', ',')))

# COMMAND ----------

# MAGIC %md ... I couldn't get the syntax the work

# COMMAND ----------

# MAGIC %md ## Coding challenge 2
# MAGIC 
# MAGIC Below is my attempt at parsing and cleaning up the problem field. The code is extremely awkward. How do I refactor this?

# COMMAND ----------

import pyspark.sql.functions as f

df_src.filter(problem_col + '.from_account is not null') \
    .select('id', f.posexplode_outer(f.split(f'{problem_col}.from_account', ','))) \
    .withColumn('col', f.regexp_replace('col', "[{} ]", "")) \
    .withColumn('col', f.when(f.col('col') == 'null', None).otherwise(f.col('col'))) \
    .withColumn('col', f.when(f.col('col') == '', None).otherwise(f.col('col'))) \
    .show(20, truncate=False)

# COMMAND ----------

# MAGIC %md ## Lastly
# MAGIC 
# MAGIC The overarching problem being how update the target table with the correct schema and merge the parsed and clean version of the data.

# COMMAND ----------


