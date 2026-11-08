# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %sql
# MAGIC -- Create catalog
# MAGIC CREATE CATALOG dlt;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create schema
# MAGIC CREATE SCHEMA dlt.etl;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create orders table from TPC-H sample data
# MAGIC CREATE TABLE dlt.etl.orders_raw
# MAGIC DEEP CLONE samples.tpch.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create customers table from TPC-H sample data
# MAGIC CREATE TABLE dlt.etl.customers_raw
# MAGIC DEEP CLONE samples.tpch.customer;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dlt.etl.orders_raw

# COMMAND ----------

# MAGIC %md
# MAGIC adding some data incemetally

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into dlt.etl.orders_raw
# MAGIC select * from samples.tpch.orders limit 15000

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dlt.etl.orders_agg_gold

# COMMAND ----------

# MAGIC %sql
# MAGIC create volume dlt.etl.landing

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/dlt/etl/landing/files")


# COMMAND ----------

dbutils.fs.cp("/databricks-datasets/definitive-guide/data/retail-data/by-day/2010-12-05.csv","/Volumes/dlt/etl/landing/files/")

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table dlt.etl.customers_raw add columns (__src_action string,__src_insert_dt timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dlt.etl.customers_raw limit 1

# COMMAND ----------

# MAGIC %sql
# MAGIC update dlt.etl.customers_raw set __src_action = 'insert', __src_insert_dt = current_timestamp() - interval '1' day
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dlt.etl.customers_bronze_scd2 where c_custkey=6

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert changed record for cust_key 6
# MAGIC
# MAGIC INSERT INTO dlt.etl.customers_raw
# MAGIC VALUES (
# MAGIC     6,
# MAGIC     'Customer#000412450',
# MAGIC     'fUD6IoGdtF',
# MAGIC     20,
# MAGIC     '30-293-696-5047',
# MAGIC     4406.28,
# MAGIC     'BUILDING',
# MAGIC     'New Changed Record',
# MAGIC     'insert',
# MAGIC     current_timestamp() - INTERVAL '1' day
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert data in Orders Raw
# MAGIC -- Setting o_orderstatus other than O, F or P
# MAGIC -- Setting o_orderprice < 0
# MAGIC
# MAGIC INSERT INTO dlt.etl.orders_raw
# MAGIC VALUES
# MAGIC (9999, 227285, 'NA', 162169.66, '1995-10-11', '1-URGENT', 'Clerk#000000432', 0, 'Demo record 1 for Expectations test'),
# MAGIC (9999, 227285, 'O', -100, '1995-10-11', '1-URGENT', 'Clerk#000000432', 0, 'Demo record 2 for Expectations test'),
# MAGIC (9999, 227285, NULL, 999, '1995-10-11', '1-URGENT', 'Clerk#000000432', 0, 'Demo record 3 for Expectations test');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert data in Customer Raw
# MAGIC -- Setting c_mktsegment as null
# MAGIC
# MAGIC INSERT INTO dlt.etl.customers_raw
# MAGIC VALUES(
# MAGIC     9999,
# MAGIC     'Customer#000412450',
# MAGIC     'fUD6IoGdtF',
# MAGIC     20,
# MAGIC     '30-293-696-5047',
# MAGIC     4406.28,
# MAGIC     NULL,
# MAGIC     'Demo record for Expectation test',
# MAGIC     'I',
# MAGIC     current_timestamp()
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from event_log ("2f9985ae-4762-47b3-983a-c59fd174bb5c")

# COMMAND ----------

dbutils.fs.cp("/databricks-datasets/definitive-guide/data/retail-data/by-day/2010-12-09.csv","/Volumes/dlt/etl/landing/files/")