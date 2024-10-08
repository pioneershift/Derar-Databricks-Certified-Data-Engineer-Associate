# Databricks notebook source
# MAGIC %sql
# MAGIC -- USE CATALOG pioneersolutionsproducts
# MAGIC USE CATALOG hive_metastore

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE employees
# MAGIC -- USING DELTA 
# MAGIC   (id INT, name STRING, salary DOUBLE);
# MAGIC
# MAGIC -- DELTA IS DEFAULT FORMAT
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO employees
# MAGIC VALUES 
# MAGIC   (1, "Adam", 3500.0),
# MAGIC   (2, "Sarah", 4020.5);
# MAGIC
# MAGIC INSERT INTO employees
# MAGIC VALUES
# MAGIC   (3, "John", 2999.3),
# MAGIC   (4, "Thomas", 4000.3);
# MAGIC
# MAGIC INSERT INTO employees
# MAGIC VALUES
# MAGIC   (5, "Anna", 2500.0);
# MAGIC
# MAGIC INSERT INTO employees
# MAGIC VALUES
# MAGIC   (6, "Kim", 6200.3)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL employees

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees/_delta_log/'

# COMMAND ----------

# MAGIC %fs ls 's3://databricks-workspace-stack-da589-bucket/unity-catalog/2335081138202215/__unitystorage/catalogs'

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table employees

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table pioneersolutionsproducts.default.employees

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE employees 
# MAGIC SET salary = salary + 100
# MAGIC WHERE name LIKE "A%"

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees/_delta_log/'

# COMMAND ----------

# MAGIC %fs head 'dbfs:/user/hive/warehouse/employees/_delta_log/00000000000000000005.json'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY Employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employees

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employees version as of 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employees@v3

# COMMAND ----------

# MAGIC %sql 
# MAGIC delete from employees

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history employees

# COMMAND ----------

# MAGIC %sql
# MAGIC restore table employees to version as of 5

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employees

# COMMAND ----------

# MAGIC %sql
# MAGIC restore table employees to version as of 5

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employees;

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe detail employees

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe history employees

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize employees
# MAGIC zorder by (id)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail employees

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history employees

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

# COMMAND ----------

# MAGIC %md
# MAGIC VACUUM operation by default take retention period as **7 days**. 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM Employees

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table employees

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

# COMMAND ----------

# MAGIC %md
# MAGIC TABLE SETUP

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE managed_default
# MAGIC   (width INT, length INT, height INT);
# MAGIC
# MAGIC INSERT INTO managed_default
# MAGIC VALUES (3 INT, 2 INT, 1 INT)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED managed_default

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/managed_default'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE external_default
# MAGIC   (width INT, length INT, height INT)
# MAGIC LOCATION 'dbfs:/mnt/demo/external_default';
# MAGIC   
# MAGIC INSERT INTO external_default
# MAGIC VALUES (3 INT, 2 INT, 1 INT)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED external_default

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/mnt/demo/external_default'

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE managed_default;
# MAGIC DROP TABLE external_default;

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/managed_default'

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/mnt/demo/external_default'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE external_default
# MAGIC   (width INT, length INT, height INT)
# MAGIC LOCATION 'dbfs:/mnt/demo/external_default';
# MAGIC   
# MAGIC INSERT INTO external_default
# MAGIC VALUES (3 INT, 2 INT, 1 INT)

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/mnt/demo/external_default'

# COMMAND ----------

# MAGIC %md
# MAGIC **CREATING SCHEMAS**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA new_default

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE EXTENDED new_default

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA custom
# MAGIC LOCATION 'dbfs:/Shared/schemas/custom.db'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE EXTENDED custom

# COMMAND ----------

# MAGIC %sql
# MAGIC USE new_default;
# MAGIC
# MAGIC CREATE TABLE managed_new_default
# MAGIC   (width INT, length INT, height INT);
# MAGIC   
# MAGIC INSERT INTO managed_new_default
# MAGIC VALUES (3 INT, 2 INT, 1 INT);
# MAGIC
# MAGIC -----------------------------------
# MAGIC
# MAGIC CREATE TABLE external_new_default
# MAGIC   (width INT, length INT, height INT)
# MAGIC LOCATION 'dbfs:/mnt/demo/external_new_default';
# MAGIC   
# MAGIC INSERT INTO external_new_default
# MAGIC VALUES (3 INT, 2 INT, 1 INT);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED managed_new_default

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED external_new_default

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE managed_new_default;
# MAGIC DROP TABLE external_new_default;

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/new_default.db/managed_new_default'

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/mnt/demo/external_new_default'

# COMMAND ----------

# MAGIC %sql
# MAGIC USE custom;
# MAGIC
# MAGIC CREATE TABLE managed_custom
# MAGIC   (width INT, length INT, height INT);
# MAGIC   
# MAGIC INSERT INTO managed_custom
# MAGIC VALUES (3 INT, 2 INT, 1 INT);
# MAGIC
# MAGIC -----------------------------------
# MAGIC
# MAGIC CREATE TABLE external_custom
# MAGIC   (width INT, length INT, height INT)
# MAGIC LOCATION 'dbfs:/mnt/demo/external_custom';
# MAGIC   
# MAGIC INSERT INTO external_custom
# MAGIC VALUES (3 INT, 2 INT, 1 INT);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED managed_custom

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED external_custom

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE managed_custom;
# MAGIC DROP TABLE external_custom;

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/Shared/schemas/custom.db/managed_custom'

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/mnt/demo/external_custom'

# COMMAND ----------

# MAGIC %md
# MAGIC **VIEWS**

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG hive_metastore;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS smartphones
# MAGIC (id INT, name STRING, brand STRING, year INT);
# MAGIC
# MAGIC INSERT INTO smartphones
# MAGIC VALUES (1, 'iPhone 14', 'Apple', 2022),
# MAGIC       (2, 'iPhone 13', 'Apple', 2021),
# MAGIC       (3, 'iPhone 6', 'Apple', 2014),
# MAGIC       (4, 'iPad Air', 'Apple', 2013),
# MAGIC       (5, 'Galaxy S22', 'Samsung', 2022),
# MAGIC       (6, 'Galaxy Z Fold', 'Samsung', 2022),
# MAGIC       (7, 'Galaxy S9', 'Samsung', 2016),
# MAGIC       (8, '12 Pro', 'Xiaomi', 2022),
# MAGIC       (9, 'Redmi 11T Pro', 'Xiaomi', 2022),
# MAGIC       (10, 'Redmi Note 11', 'Xiaomi', 2021)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW view_apple_phones
# MAGIC AS  SELECT * 
# MAGIC     FROM smartphones 
# MAGIC     WHERE brand = 'Apple';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM view_apple_phones;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMP VIEW temp_view_phones_brands
# MAGIC AS  SELECT DISTINCT brand
# MAGIC     FROM smartphones;
# MAGIC
# MAGIC SELECT * FROM temp_view_phones_brands;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE GLOBAL TEMP VIEW global_temp_view_latest_phones
# MAGIC AS SELECT * FROM smartphones
# MAGIC     WHERE year > 2020
# MAGIC     ORDER BY year DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES in GLOBAL_TEMP;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.global_temp_view_latest_phones

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW global_temp.global_temp_view_latest_phones;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `hive_metastore`; select * from `new_default`.`global_temp_view_latest_phones` limit 100;
