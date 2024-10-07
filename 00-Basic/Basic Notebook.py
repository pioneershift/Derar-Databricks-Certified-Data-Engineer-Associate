# Databricks notebook source
print('Hello World')

# COMMAND ----------

# MAGIC %sql
# MAGIC select 'HELLO'

# COMMAND ----------

# MAGIC %md
# MAGIC #Title 1
# MAGIC ##Title 2
# MAGIC ###Title 3
# MAGIC
# MAGIC text with **bold** and *italiced* in it
# MAGIC
# MAGIC Ordered List
# MAGIC 1. One
# MAGIC 2. Two
# MAGIC 3. Three
# MAGIC
# MAGIC Unordered List
# MAGIC * Bob
# MAGIC * Jane
# MAGIC * Henry
# MAGIC
# MAGIC Images:
# MAGIC ![Databricks Associate Badge](https://miro.medium.com/v2/resize:fit:640/format:webp/1*QqZwR9XLOzCnuv9_Jqs0UA.png)
# MAGIC
# MAGIC tables:
# MAGIC | S No|Name|
# MAGIC |----|-----|
# MAGIC |1|Deepankar|
# MAGIC |2|Henry|
# MAGIC
# MAGIC Links: <a href="https://www.databricks.com/learn/certification/data-engineer-associate">Data Engineer Associate </a>

# COMMAND ----------

# MAGIC %run
# MAGIC ./Includes/Setup

# COMMAND ----------

print(full_name)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls '/databricks-datasets'

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.ls('/databricks-datasets/')

# COMMAND ----------

display(full_name)

# COMMAND ----------

full_name

# COMMAND ----------

print(full_name)

# COMMAND ----------

filelist = dbutils.fs.ls("/databricks-datasets/")

# COMMAND ----------

print(filelist)
display(filelist)

# COMMAND ----------


