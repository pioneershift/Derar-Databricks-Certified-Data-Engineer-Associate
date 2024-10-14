# Databricks notebook source
# MAGIC %md
# MAGIC All files copied to *dbfs:/mnt/demo-datasets/bookstore*

# COMMAND ----------

dataset_bookstore='dbfs:/mnt/demo-datasets/bookstore'
data_catalog = 'hive_metastore'
spark.conf.set(f"dataset.bookstore", dataset_bookstore)
spark.sql(f"USE CATALOG hive_metastore")

# COMMAND ----------

display(dbutils.fs.ls(f'{dataset_bookstore}/'))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reading a JSON File
# MAGIC SELECT *,input_file_name() FROM json.`${dataset.bookstore}/customers-json/`; -- 1700 records - query the folder
# MAGIC SELECT *,input_file_name() FROM json.`${dataset.bookstore}/customers-json/export_*.json`; -- 1700 records - query the file with wildcard
# MAGIC SELECT *,input_file_name() FROM json.`${dataset.bookstore}/customers-json/export_001.json`; -- 300 records - query a specific file

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reading a JSON File
# MAGIC SELECT customer_id,email,profile FROM json.`${dataset.bookstore}/customers-json/`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reading a Binary File
# MAGIC SELECT * FROM binaryFile.`${dataset.bookstore}/customers-json/`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reading a TEXT File
# MAGIC SELECT * FROM text.`${dataset.bookstore}/books-csv/`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reading a parquet file
# MAGIC SELECT * FROM PARQUET.`${dataset.bookstore}/orders/`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reading a CSV File
# MAGIC SELECT * FROM csv.`${dataset.bookstore}/books-csv/`
# MAGIC ;
# MAGIC -- does not mention the schema

# COMMAND ----------

# MAGIC %md
# MAGIC For reading CSV files with specific options (like delimiter), you would typically use the spark.read option in a Databricks notebook cell using PySpark or use the CREATE TABLE statement in SQL with options and then query that table. Since your environment is Databricks SQL and you're encountering a syntax error with OPTIONS, you might consider an alternative approach if you're working within a SQL notebook.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reading a CSV File with Options
# MAGIC CREATE TABLE books_csv_options
# MAGIC (book_id String, title String, author String, category String, price Double)
# MAGIC USING csv
# MAGIC OPTIONS (
# MAGIC   header = 'true',
# MAGIC   delimiter = ';'
# MAGIC )
# MAGIC LOCATION "${dataset.bookstore}/books-csv"
# MAGIC -- ensure you have setup a catalog to run the query
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from books_csv_options

# COMMAND ----------

# MAGIC %md
# MAGIC The table is pointing to existing datafiles. No new datafiles is created as part of this unlike normal create table query (which is a delta table). 
# MAGIC
# MAGIC if you add new datafile with same format, you can see the new data if you refresh the table. 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended  books_csv_options

# COMMAND ----------

display(dbutils.fs.ls(f'{dataset_bookstore}/books-csv'))

# COMMAND ----------

# csvschema=StructType()
mycsv=spark.read.csv(f'{dataset_bookstore}/books-csv',sep=';',header=True)
display(mycsv)
mycsv.write.mode('append').csv(f'{dataset_bookstore}/books-csv')

# COMMAND ----------

display(dbutils.fs.ls(f'{dataset_bookstore}/books-csv'))

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from books_csv_options -- 24 records event thoough the table has more records.

# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table books_csv_options

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from books_csv_options -- 44 records after refresh.
# MAGIC -- non delta table does not automatically provided updated data
# MAGIC -- non delta table, you cannot time travel
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC * A Non Delta table is created using simple CREATE TABLE *tablename* with LOCATION option.
# MAGIC * A Delta table is created when you use CTAS i.e. CREATE TABLE AS SELECT statement

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT *,input_file_name() FROM json.`${dataset.bookstore}/customers-json/`;
# MAGIC
# MAGIC CREATE TABLE customer_json_delta AS
# MAGIC SELECT * FROM json.`${dataset.bookstore}/customers-json/`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED customer_json_delta;

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/user/hive/warehouse/customer_json_delta'))

# COMMAND ----------

# MAGIC %md
# MAGIC FOR CSV files, we can face issue creating table using CTAS as it does not allow to give specifications to the table. 
# MAGIC In such cases, you can create a temporary table from CSV specifing the format and then load to new table using CTAS to temporary table. Thus creating a delta table from CSV with schema specification.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP  TABLE IF EXISTS books_csv_temp;
# MAGIC
# MAGIC CREATE TEMPORARY TABLE books_csv_temp
# MAGIC (book_id String, title String, author String, category String, price Double)
# MAGIC USING csv
# MAGIC OPTIONS (
# MAGIC   header = 'true',
# MAGIC   delimiter = ';'
# MAGIC )
# MAGIC LOCATION "${dataset.bookstore}/books-csv";
# MAGIC
# MAGIC CREATE TABLE books_csv_delta AS 
# MAGIC SELECT * FROM books_csv_temp;
# MAGIC
# MAGIC DESCRIBE EXTENDED books_csv_delta;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC WRITING TO TABLES
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ORDERS AS
# MAGIC SELECT * FROM parquet.`${dataset.bookstore}/orders`;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders;

# COMMAND ----------

# MAGIC %md
# MAGIC You can update the data in the table using CORTAS - CREAT OR REPLACE TABLE AS 
# MAGIC
# MAGIC * This can be used for table not yet created
# MAGIC * This is faster process
# MAGIC * This is atomic process
# MAGIC * This is more efficient process
# MAGIC
# MAGIC
# MAGIC You can also update a table using INSERT OVERWRITE *table* 
# MAGIC
# MAGIC * This can only overwrite an existing table
# MAGIC * This will only insert records which matches the format of existing table. 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ORDERS AS
# MAGIC SELECT * FROM parquet.`${dataset.bookstore}/orders`;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY ORDERS;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE ORDERS
# MAGIC SELECT * FROM parquet.`${dataset.bookstore}/orders`;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY ORDERS

# COMMAND ----------

# MAGIC %md
# MAGIC UPDATING TABLE
# MAGIC
# MAGIC * INSERT INTO TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO TABLE ORDERS
# MAGIC SELECT * FROM parquet.`${dataset.bookstore}/orders`;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY ORDERS;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders

# COMMAND ----------

# MAGIC %md
# MAGIC INSERT RECORDS CAN ADD DUPLICACY. YOU CAN DO MERGE TO AVOID THIS

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW customer_updates AS
# MAGIC SELECT * FROM json.`${dataset.bookstore}/customers-json-new`
# MAGIC ;
# MAGIC
# MAGIC MERGE INTO customers c
# MAGIC USING customer_updates cu
# MAGIC ON c.customer_id = cu.customer_id
# MAGIC WHEN MATCHED AND c.email IS NULL AND cu.email IS NOT NULL
# MAGIC   THEN UPDATE SET c.email = cu.email, c.updated = cu.updated
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW customer_updates AS
# MAGIC SELECT * FROM json.`${dataset.bookstore}/customers-json-new`
# MAGIC ;
# MAGIC
# MAGIC MERGE INTO customers c
# MAGIC USING customer_updates cu
# MAGIC ON c.customer_id = cu.customer_id
# MAGIC WHEN MATCHED AND c.email IS NULL AND cu.email IS NOT NULL
# MAGIC   THEN UPDATE SET c.email = cu.email, c.updated = cu.updated
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW book_updates (book_id string, title	string, author	string, category	string, price	double)
# MAGIC USING CSV
# MAGIC OPTIONS 
# MAGIC (
# MAGIC   path = '${dataset.bookstore}/books-csv-new',
# MAGIC   header = "true",
# MAGIC   delimiter = ';'
# MAGIC )
# MAGIC ;
# MAGIC
# MAGIC select * from book_updates;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO books b
# MAGIC USING book_updates bu
# MAGIC On b.book_id = bu.book_id
# MAGIC WHEN NOT MATCHED  and bu.category = 'Computer Science' THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM BOOKS

# COMMAND ----------

# MAGIC %md
# MAGIC ## ADVANCE HANDS ON

# COMMAND ----------

# MAGIC %md
# MAGIC ### QUERY TO JSON FILE 

# COMMAND ----------

# MAGIC %md
# MAGIC Here the profile is of text data type. You can access the fields using colon :

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC customer_id,
# MAGIC email,
# MAGIC profile:first_name,
# MAGIC profile:last_name,
# MAGIC profile:gender,
# MAGIC profile:address:street,
# MAGIC profile:address:city,
# MAGIC profile:address:country,
# MAGIC profile:address,
# MAGIC profile
# MAGIC  from customers;

# COMMAND ----------

# MAGIC %md
# MAGIC Another way to access json attributes is by creating a struct type field using from_json() function. You need to mention a sample structure to this function and it will create a structure based on the sample with non null values

# COMMAND ----------

# MAGIC %sql
# MAGIC select from_json(profile) from customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC select from_json(profile
# MAGIC , schema_of_json('{"first_name":"Inger","last_name":"Goodlip","gender":"Male","address":{"street":"898 Graceland Lane","city":"Malysheva","country":"Russia"}}')
# MAGIC ) 
# MAGIC AS profile_struct from customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW parsed_customer AS
# MAGIC select customer_id,from_json(profile, schema_of_json('{"first_name":"Inger","last_name":"Goodlip","gender":"Male","address":{"street":"898 Graceland Lane","city":"Malysheva","country":"Russia"}}')) AS profile_struct from customers;
# MAGIC
# MAGIC describe parsed_customer;

# COMMAND ----------

# MAGIC %md
# MAGIC Once you have created json field as struct type you can access the fields using . notation
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select customer_id, profile_struct.* from parsed_customer

# COMMAND ----------

# MAGIC %sql
# MAGIC select customer_id,
# MAGIC     profile_struct.first_name,
# MAGIC     profile_struct.last_name,
# MAGIC     profile_struct.address.city from parsed_customer

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW parsed_customer_proper AS
# MAGIC with parsed_customer_cte as (select customer_id,from_json(profile, schema_of_json('{"first_name":"Inger","last_name":"Goodlip","gender":"Male","address":{"street":"898 Graceland Lane","city":"Malysheva","country":"Russia"}}')) AS profile_struct from customers)
# MAGIC select customer_id, profile_struct.* from parsed_customer_cte;
# MAGIC
# MAGIC describe parsed_customer_proper;

# COMMAND ----------

# MAGIC %sql
# MAGIC select customer_id, address, first_name from parsed_customer_proper;

# COMMAND ----------

# MAGIC %md
# MAGIC Reading JSON with array of elements for a row - Use EXPLODE

# COMMAND ----------

# MAGIC %sql
# MAGIC select customer_id,order_id, books from orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC select customer_id,order_id, explode(books) as books from orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC with book_cte as (select customer_id,order_id, explode(books) as book from orders)
# MAGIC select customer_id,order_id, book.book_id,book.quantity,book.subtotal from book_cte
# MAGIC order by 1,2,3,4,5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select customer_id, order_id, books.book_id from orders order by customer_id

# COMMAND ----------

# MAGIC %md
# MAGIC For rows where there are repeated values, you can combine them into array using collect_set() function.
# MAGIC It can contain array with repeated values in sub arrays so you can flatten() it and use array_distinct() to get the distinct values.

# COMMAND ----------

# MAGIC %sql
# MAGIC select customer_id
# MAGIC   ,collect_list(order_id) as order_list
# MAGIC   , collect_set(order_id) as order_set
# MAGIC   , collect_set(books.book_id) as book_id_set 
# MAGIC   , flatten(collect_set(books.book_id)) as book_id_flatten
# MAGIC   , array_distinct(flatten(collect_set(books.book_id))) as book_id_flat_distinct_array
# MAGIC   from orders group by customer_id order by customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC with orders_flat as (
# MAGIC   select order_id, customer_id, quantity, total, explode(books) as books from orders
# MAGIC   order by customer_id, order_id
# MAGIC )
# MAGIC select * from orders_flat o
# MAGIC inner join books b on o.books.book_id = b.book_id

# COMMAND ----------

# MAGIC %md
# MAGIC You can also do UNION, INTERSECT, MINUS between 2 sets of data in the query

# COMMAND ----------

# MAGIC %md
# MAGIC PIVOT - change data perspective. Columns can be set as rows and row values can be set as columns. 
# MAGIC Specify pivot after the table name or subquery.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC with order_flat as (select customer_id, explode(books) as books from orders)
# MAGIC , order_for_pivot (
# MAGIC   select customer_id
# MAGIC       , books.book_id as bookid
# MAGIC       , books.quantity as quantity
# MAGIC       , books.subtotal 
# MAGIC   from order_flat
# MAGIC )
# MAGIC select * from order_for_pivot
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC with order_flat as (select customer_id, explode(books) as books from orders)
# MAGIC , order_for_pivot (
# MAGIC   select customer_id , books.book_id as bookid , books.quantity as quantity, books.subtotal from order_flat
# MAGIC )
# MAGIC select * from order_for_pivot
# MAGIC PIVOT (
# MAGIC   sum(quantity) for bookid in (
# MAGIC     'B06','B10','B08','B07','B03','B05','B02','B11','B09','B12','B01','B04'
# MAGIC   )
# MAGIC )
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Higher order Function and UDF
# MAGIC * FILTER *filter(array, lambda_function): Returns an array of elements for which the lambda function returns true.*
# MAGIC * TRANSFORM *transform(array, lambda_function): Applies the lambda function to each element in the input array and returns an array of the result.*
# MAGIC * EXISTS *exists(array, lambda_function): Returns true if the lambda function returns true for any element in the array.*
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select books.book_id, books.quantity,
# MAGIC   FILTER(books, i -> i.quantity >= 2) as multiple_copies,
# MAGIC   * from orders

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 
# MAGIC (
# MAGIC   select books.book_id, books.quantity,
# MAGIC   FILTER(books, i -> i.quantity >= 2) as multiple_copies,
# MAGIC   * from orders
# MAGIC ) res
# MAGIC WHERE size(multiple_copies) >0;

# COMMAND ----------

# MAGIC %sql
# MAGIC select order_id, books,
# MAGIC TRANSFORM(books, i -> i.subtotal*0.8) as subtotal_after_discount
# MAGIC from orders;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##UDF
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION get_url(email STRING)
# MAGIC RETURNS STRING
# MAGIC
# MAGIC RETURN CONCAT ('http://www.',split(email, '@')[1])
# MAGIC ;
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION get_site(email STRING)
# MAGIC RETURNS STRING
# MAGIC
# MAGIC RETURN  CASE 
# MAGIC           WHEN email like '%.com' THEN 'Commercial Business'
# MAGIC           WHEN email like '%.org' THEN 'Non Profit Organisation'
# MAGIC           WHEN email like '%.edu' THEN 'Educational Institute'
# MAGIC           WHEN email like '%.gov' THEN 'Government Sector'
# MAGIC           ELSE CONCAT('Unknow Extenstion ',SPLIT(email,'@')[1])
# MAGIC         END;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select email,get_url(email) as url_address, get_site(email) as site_type from customers

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FUNCTION GET_URL

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FUNCTION EXTENDED GET_URL

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION get_url;
# MAGIC DROP FUNCTION get_site;
