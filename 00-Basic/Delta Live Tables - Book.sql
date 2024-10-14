-- Databricks notebook source
CREATE OR REFRESH STREAMING LIVE TABLE books_bronze
COMMENT 'The raw book data ingested from CDC feed'
AS SELECT * FROM cloud_files('${datasets.path}/books-cdc','json' )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC SILVER LAYER CDC

-- COMMAND ----------

CREATE OR REPLACE STREAMING LIVE TABLE books_silver;

APPLY CHANGES INTO LIVE.books_silver
  FROM STREAM(LIVE.books_bronze)
  KEYS(book_id)
APPLY AS DELETE WHEN row_status = 'DELETE'
SEQUENCE BY row_time
COLUMNS * EXCEPT (row_status, row_time)


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##GOLD
-- MAGIC
-- MAGIC *this is not a streaming table since its source table has done delete and update. For streaming tables, it should only be append only*

-- COMMAND ----------

CREATE LIVE TABLE author_count_stats
  COMMENT 'No of books per auther'
AS 
  SELECT author, count(*) as book_count,  current_timestamp() as updated_timestamp
  FROM LIVE.books_silver
  GROUP BY author

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##DLT Views
-- MAGIC
-- MAGIC *DLF views are temporary views which are only part of  the DLT pipeline and not available outside.*

-- COMMAND ----------

CREATE OR REPLACE LIVE VIEW  book_sales
AS 
 SELECT b.title, o.quantity 
 FROM (
  select *, explode(books) AS book
  FROM LIVE.orderes_cleaned
 ) o
 INNER JOIN LIVE.books_silver b
 ON o.book.book_id = b.book_id
