# Databricks notebook source
# MAGIC %md 
# MAGIC **READ STREAMING DATA**
# MAGIC
# MAGIC `streamDF = spark.readStream.table("InputTable")`
# MAGIC
# MAGIC **WRITE STREAMING DATAFRAME**
# MAGIC
# MAGIC `streamDF.writeStream.trigger(processingTime = '2 minutes').outputMode('append').option('checkpointLocation','/path').table('OutputTable')`
# MAGIC

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

spark.readStream.table("books").createOrReplaceTempView('book_streaming_temp_vw')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from book_streaming_temp_vw;

# COMMAND ----------

# MAGIC %sql
# MAGIC select author, count(book_id) as noOfBooks
# MAGIC from book_streaming_temp_vw
# MAGIC group by author;

# COMMAND ----------

# MAGIC %md
# MAGIC SORTING not supported for streaming data but you can use windowing or watermarking for this purpose

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from book_streaming_temp_vw order by author

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view author_count_t_vw as 
# MAGIC   select author, count(book_id) as noOfBooks
# MAGIC   from book_streaming_temp_vw
# MAGIC   group by author
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC A Temporary view created from a streaming table is also a streaming view

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from author_count_t_vw;

# COMMAND ----------

# MAGIC %md
# MAGIC Now you can write the transformed streaming data to an output table as a streaming data

# COMMAND ----------

(spark.table('author_count_t_vw')
    .writeStream
    .trigger(processingTime = '4 seconds')
    .outputMode('complete')
    .option('checkpointLocation','dbfs:/mnt/demo/author_counts_checkpoint')
    .table('author_counts')
)

# COMMAND ----------

# MAGIC %md
# MAGIC You can read from this table directly as a non streaming source

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from author_counts order by author

# COMMAND ----------

# MAGIC %md
# MAGIC Lets test adding new data in the source

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values ("B19", "Introduction to Modeling and Simulation", "Mark W. Spong", "Computer Science", 25),
# MAGIC         ("B20", "Robot Modeling and Control", "Mark W. Spong", "Computer Science", 30),
# MAGIC         ("B21", "Turing's Vision: The Birth of Computer Science", "Chris Bernhardt", "Computer Science", 35)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values ("B22", "Introduction to Modeling and Simulation", "Deepankar Barua", "Computer Science", 25),
# MAGIC         ("B23", "Robot Modeling and Control", "Deepankar Barua", "Computer Science", 30),
# MAGIC         ("B24", "Turing's Vision: The Birth of Computer Science", "Mandhul Mehta", "Computer Science", 35)

# COMMAND ----------

# MAGIC %md
# MAGIC with trigger as availableNow, the job runs once and processes all data then it stops as against the processingTime parameter where job runs on regular basis to write new data.
# MAGIC awaitTermination() - Keeps anyother job in this notebook to wait untill the incremental batch write is successful

# COMMAND ----------

(spark.table('author_count_t_vw')
    .writeStream
    .trigger(availableNow=True)
    .outputMode('complete')
    .option('checkpointLocation','dbfs:/mnt/demo/author_counts_checkpoint')
    .table('author_counts')
    .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from author_counts

# COMMAND ----------

# List all active streams
for stream in spark.streams.active:
    print(stream.id, stream.name, stream.isActive)

# Stop a specific stream by ID or stop all
# spark.streams.active[0].stop()  # Stopping the first stream as an example
# or

# for stream in spark.streams.active:
#     stream.stop()  # Stopping all active streams

# COMMAND ----------

# Count the number of active streaming queries
num_active_streams = len(spark.streams.active)
print(f"Number of active streaming queries: {num_active_streams}")

# COMMAND ----------

# MAGIC %md
# MAGIC #AUTOLOADER
# MAGIC

# COMMAND ----------

files = dbutils.fs.ls(f'{dataset_bookstore}/orders-raw')
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC Autoloader reads and writes cloud file
# MAGIC
# MAGIC spark.readStream                            *read files*
# MAGIC
# MAGIC     .format('cloudFiles')                   *cloudFiles indicate its autoload stream*
# MAGIC
# MAGIC     .option('cloudFiles.format','parquet')  *format of cloudfile*
# MAGIC
# MAGIC     .option('cloudFiles.schemaLocation','dbfs:/mnt/demo/orders_checkpoint')  *directory location where autoload stores the inferred schema*
# MAGIC     
# MAGIC .load(f'{dataset_bookstore}/orders-raw')              *location of datasource file*
# MAGIC
# MAGIC .writeStream
# MAGIC     
# MAGIC     .option('checkpointLocation','dbfs:/mnt/demo/orders_checkpoint')  *location to store data in target table*
# MAGIC
# MAGIC     .table('orders_update')                 *table name*
# MAGIC

# COMMAND ----------

(spark.readStream
    .format('cloudFiles')
    .option('cloudFiles.format','parquet')
    .option('cloudFiles.schemaLocation','dbfs:/mnt/demo/orders_checkpoint')
.load(f'{dataset_bookstore}/orders-raw')
.writeStream.option('checkpointLocation','dbfs:/mnt/demo/orders_checkpoint')
    .table('orders_update'))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_update

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders_update

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table orders_update;

# COMMAND ----------

dbutils.fs.rm('dbfs:/mnt/demo/orders_checkpoint',True)

# COMMAND ----------

# MAGIC %md
# MAGIC #Multi hop Architecture
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Source Files

# COMMAND ----------

display(dbutils.fs.ls(f'{dataset_bookstore}/orders-raw'))

# COMMAND ----------

# MAGIC %md
# MAGIC Create an autoloader for source data read

# COMMAND ----------

sourceStreamDf=(spark.readStream
                .format('cloudFiles')
                .option('cloudFiles.format','parquet')
                .option('cloudFiles.schemaLocation',f'{dataset_bookstore}/orders-raw')
                .load(f'{dataset_bookstore}/orders-raw')
            )
sourceStreamDf.createOrReplaceTempView('order_raw_temp')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from order_raw_temp;

# COMMAND ----------

# MAGIC %md
# MAGIC Create a temporary view who enhances the data and stores in the temp view.
# MAGIC This is a streaming source

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW orders_tmp as
# MAGIC select * , current_timestamp() as arrival_time, input_file_name() as source_file
# MAGIC from order_raw_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_tmp

# COMMAND ----------

# MAGIC %md
# MAGIC Create Bronze Layer Table - order_bronze

# COMMAND ----------

(spark.table('orders_tmp')
    .writeStream
    .format('delta')
    .option('checkpointLocation','dbfs:/mnt/demo/checkpoints/orders-bronze')
    .outputMode('append')
    .table('orders_bronze')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Lookup Table

# COMMAND ----------

lookupDf = (spark.read
            .format('json')
            .load(f'{dataset_bookstore}/customers-json'))
lookupDf.createOrReplaceTempView('customers_lookup')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers_lookup

# COMMAND ----------

# MAGIC %md
# MAGIC Enriching the Bronze Table

# COMMAND ----------

bronzedf = (spark.readStream.table('orders_bronze'))
bronzedf.createOrReplaceTempView('orders_bronze_temp')
              


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW order_enriched_tmp as 
# MAGIC select order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name, cast(from_unixtime(order_timestamp,'yyyy-MM-dd HH:mm:ss') AS timestamp) as order_timestamp, books
# MAGIC from orders_bronze_temp o
# MAGIC INNER join customers_lookup c
# MAGIC   on o.customer_id = c.customer_id
# MAGIC WHERE quantity > 0

# COMMAND ----------

# MAGIC %md
# MAGIC Enriched Streaming Table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from order_enriched_tmp

# COMMAND ----------

# MAGIC %md
# MAGIC Writing Enriched Bronze Data to Silver layer as streaming write

# COMMAND ----------

(spark.table('order_enriched_tmp')
    .writeStream
    .format('delta')
    .option('checkpointLocation','dbfs:/mnt/demo/checkpoints/orders_silver')
    .outputMode('append')
    .table('orders_silver')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_silver

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC Creating GOLD layer table

# COMMAND ----------

# MAGIC %md
# MAGIC Reading silver data as streaming data and store it in a temporary view

# COMMAND ----------

silvertmpdf = (spark.readStream.table('orders_silver'))
silvertmpdf.createOrReplaceTempView('order_silvers_tmp')

# COMMAND ----------

# MAGIC %md
# MAGIC Create a temporary streaming table using enriched data

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view daily_customer_bookstore as
# MAGIC select customer_id,f_name, l_name, date_trunc("DD", order_timestamp) as order_date, sum(quantity) as book_counts
# MAGIC from order_silvers_tmp
# MAGIC group by customer_id,f_name, l_name, date_trunc("DD", order_timestamp)

# COMMAND ----------

# MAGIC %md
# MAGIC Write the Temporary Gold Data into Gold Layer

# COMMAND ----------

(spark.table('daily_customer_bookstore')
    .writeStream
    .format('delta')
    .option('checkpointLocation','dbfs:/mnt/demo/checkpoints/daily_customer_books')
    .trigger(availableNow = True)
    .table('daily_customer_books')
 )

# COMMAND ----------

# MAGIC %md
# MAGIC Aggregated data needs to be written as outputMode='complete'
# MAGIC
# MAGIC *Please note that while complete mode works well for certain use cases, it might not be the most efficient for large datasets or tables where only a small portion of the data changes between triggers. For such cases, using append or update modes might be more effici*
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC `trigger(availableNow = True)` results in steam to write once in batch for all jobs and close the job. 
# MAGIC this query has to be run every time we need to refresh the data. 

# COMMAND ----------

(spark.table('daily_customer_bookstore')
    .writeStream
    .format('delta')
    .outputMode('complete')
    .option('checkpointLocation','dbfs:/mnt/demo/checkpoints/daily_customer_books')
    .trigger(availableNow = True)
    .table('daily_customer_books')
 )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from daily_customer_books;

# COMMAND ----------

load_new_data(all=True)

# COMMAND ----------

for s in spark.streams.active:
    print('Stopping the stream:'+s.id)
    s.stop()
    s.awaitTerminatio()
