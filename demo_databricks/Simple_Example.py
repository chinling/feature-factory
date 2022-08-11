# Databricks notebook source
# MAGIC %md
# MAGIC # Basic Introduction
# MAGIC The following is a very basic introduction to demonstrate the power of feature factory once it's configured and deployed.

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql import functions as F
from feature_factory.framework.feature_factory.helpers import Helpers
from feature_factory.demo.channel_demo_store import Store
from feature_factory.framework.feature_factory.data import Joiner

spark.conf.set("spark.sql.shuffle.partitions", 96*2)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Setup 
# MAGIC 
# MAGIC #### Don't want to make this permanent; for purpose of demo, only

# COMMAND ----------

# MAGIC %md
# MAGIC #### TODO: Manual step
# MAGIC 
# MAGIC Copy the following 4 CSV files into the target Databricks location (change the map accordingly)

# COMMAND ----------

map = {
  '/FileStore/tables/cling/tomes_tpcds_delta_1tb_store_sales_enhanced.csv': 'tomes_tpcds_delta_1tb_store_sales_enhanced',
  '/FileStore/tables/cling/tomes_tpcds_delta_1tb_store.csv': 'tomes_tpcds_delta_1tb_store',
  '/FileStore/tables/cling/tomes_tpcds_delta_1tb_item.csv': 'tomes_tpcds_delta_1tb_item',
  '/FileStore/tables/cling/tomes_tpcds_delta_1tb_inventory.csv': 'tomes_tpcds_delta_1tb_inventory'
}

# COMMAND ----------

# File location and type
for file_location, temp_table_name in map.items():
  file_type = file_location[-3:]
  
  # CSV options
  infer_schema = "true"
  first_row_is_header = "true"
  delimiter = ","

  # The applied options are for CSV files. For other file types, these will be ignored.
  df = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .option("header", first_row_is_header) \
    .option("sep", delimiter) \
    .load(file_location)
  
  display(df)

  df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Getting some basic features

# COMMAND ----------

# Istantiate store
store = Store(_snapshot_date = "1998-01-31")

# Get The feature factory
ff = store.ff

# Grab some sales features
mult_features, base_features = store.Sales().get_all()

# Build a base dataframe from cores/sources
store_sales_df = store.get_data('sources.store_sales').filter(col('ss_store_sk').isNotNull()).alias('store_sales')
item_df = store.get_data('sources.item').alias('item')
base_df = (
  store_sales_df
  .join(item_df.withColumnRenamed('i_item_sk', 'ss_item_sk'), ['ss_item_sk'])
  .select('store_sales.*', 'item.i_category')
)

# Build the Features Dataframe
feature_df = ff.append_features(base_df, groupBy_cols = ['ss_store_sk'], feature_sets=[mult_features])

# COMMAND ----------

display(feature_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Now Let's Use a Multiplier
# MAGIC Let's take a look at the configs to see what kind of time helpers we're going to get

# COMMAND ----------

store.config.get_config('time_helpers').configs

# COMMAND ----------

# MAGIC %md
# MAGIC As can be seen above we have the 1m, 3m, 6m, and 12m date_filter ranges

# COMMAND ----------

time_multipliers = store.get_daterange_multiplier()
mult_by_time_features = mult_features.multiply(time_multipliers, "STORE")
feature_df = ff.append_features(base_df, groupBy_cols = ['ss_store_sk'], feature_sets=[mult_features, mult_by_time_features])
display(feature_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Categorical Multiplier
# MAGIC Now let's assume we have several cateogical columns for which we want to calculate aggregates.
# MAGIC 
# MAGIC The categorical multiplier allows you to either specific to which columns you wish to apply the multiplier as well as a minimum distinct values count `n` and an ignore list of columns and it will efficiently find all the columns with < n distinct values

# COMMAND ----------

categorical_multiplier = Helpers().get_categoricals_multiplier(df = store.get_data('sources.item'), col_list = ['i_category'])
mult_by_cat_features = mult_features.multiply(categorical_multiplier, "STORE")
feature_df = ff.append_features(base_df, groupBy_cols = ['ss_store_sk'], feature_sets=[mult_features, mult_by_time_features, mult_by_cat_features])
display(feature_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trending Features
# MAGIC Often times it's powerful to identify trends and simple projections into the future to help us make predictions. A trending Feature Family has been created to enable this easily. It fits a line to the trend given some period or periods and provides the slope and y-intercept such that you may solve for x to find any value in the future. `y = mx + b`

# COMMAND ----------

# In the following example, we're going to get the trend lines for the past 4 weeks and the past 12m. We can then do comparisons between the two trends and see if some feature[s] is[are] improving or getting worse. We can also use these trend lines for simple projections n perioids into the future.

trend_features = store.Trends(mult_features, [["1w","4w"],["1m","12m"]]).get_all()

# COMMAND ----------

# MAGIC %md
# MAGIC Notice there are many more features in trend_features than there are in the output df. This is because most of the features are intermediate/temporary features and are culled before the final output; they are simply used for fitting the line to the trend.

# COMMAND ----------

trend_features.features

# COMMAND ----------

feature_df = ff.append_features(base_df, groupBy_cols = ['ss_store_sk'], feature_sets=[mult_features, trend_features])
display(feature_df)
