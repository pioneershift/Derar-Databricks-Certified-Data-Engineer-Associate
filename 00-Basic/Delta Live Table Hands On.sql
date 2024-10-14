-- Databricks notebook source
-- MAGIC %md
-- MAGIC #DELTA LIVE TABLES

-- COMMAND ----------

-- SET datasets.path=dbfs:/mnt/demo-datasets/bookstore;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/mnt/demo-datasets/bookstore'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##BRONZE LAYER TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###orders_raw

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE orders_raw
COMMENT 'The raw book order ingested from orders-raw'
AS SELECT * FROM  cloud_files('${datasets.path}/orders-raw','parquet', map('schema','order_id String, order_timestamp Long, customer_id string, quantity Long'))


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###customers

-- COMMAND ----------

CREATE OR REPLACE LIVE TABLE customers
COMMENT 'The customer lookup table ingested from customers-json'
AS SELECT * FROM json.`${datasets.path}/customers-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##SILVER LAYER TABLE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #orders_cleaned

-- COMMAND ----------


CREATE OR REPLACE STREAMING LIVE TABLE orderes_cleaned (
  CONSTRAINT valid_order_number EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'The cleaned book orders with valid order id'
AS
  SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
  cast(from_unixtime(o.order_timestamp,'yyyy-MM-dd HH:mm:ss') AS timestamp) AS order_timestamp,
  c.profile:address:country as country
  FROM STREAM(LIVE.orders_raw) o
  LEFT JOIN LIVE.customers c
    on o.customer_id = c.customer_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC >> Constraint violation
-- MAGIC
-- MAGIC | **`ON VIOLATION`** | Behavior |
-- MAGIC | --- | --- |
-- MAGIC | **`DROP ROW`** | Discard records that violate constraints |
-- MAGIC | **`FAIL UPDATE`** | Violated constraint causes the pipeline to fail  |
-- MAGIC | Omitted | Records violating constraints will be kept, and reported in metrics |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #GOLD TABLE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##customer_books

-- COMMAND ----------


CREATE OR REPLACE LIVE TABLE cn_daily_customer_books
COMMENT 'Daily number of books per customer in China'
AS 
  SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
  FROM LIVE.orderes_cleaned
  WHERE country = 'China'
  GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)


-- COMMAND ----------


CREATE OR REPLACE LIVE TABLE fr_daily_customer_books
COMMENT 'Daily number of books per customer in France'
AS 
  SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
  FROM LIVE.orderes_cleaned
  WHERE country = 'France'
  GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)


-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/mnt/demo-datasets/bookstore/autoloader/'))
-- MAGIC display(dbutils.fs.ls('dbfs:/mnt/demo-datasets/bookstore/checkpoints/'))
-- MAGIC display(dbutils.fs.ls('dbfs:/mnt/demo-datasets/bookstore/system/'))
-- MAGIC display(dbutils.fs.ls('dbfs:/mnt/demo-datasets/bookstore/tables/'))

-- COMMAND ----------

select * from hive_metastore.demo_bookstore_dlt_db.fr_daily_customer_books
