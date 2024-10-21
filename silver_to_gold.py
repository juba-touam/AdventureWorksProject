# Databricks notebook source
dbutils.fs.ls('/mnt/silver/SalesLT')

# COMMAND ----------

dbutils.fs.ls('/mnt/silver/SalesLT')  

# COMMAND ----------

input_path='/mnt/silver/SalesLT/Address'

# COMMAND ----------

df=spark.read.format('delta').load(input_path)
display(df)

# COMMAND ----------

column_names = df.columns
column_names

# COMMAND ----------

# MAGIC %md
# MAGIC Doing the Transformation for All Tables

# COMMAND ----------

tables = []

for i in dbutils.fs.ls("/mnt/silver/SalesLT/"):
    tables.append(i.name.split("/")[0])


# COMMAND ----------

tables

# COMMAND ----------


for name in tables:
    path = '/mnt/silver/SalesLT/' + name
    print(path)
    df = spark.read.format('delta').load(path)

    column_names = df.columns

    for old_col_name in column_names:
       new_col_name="".join(["_" + char if char.isupper() and not old_col_name[i - 1].isupper() else char for i, char in enumerate(old_col_name)]).lstrip("_")

       df=df.withColumnRenamed(old_col_name, new_col_name)
    
    output_path = '/mnt/gold/SalesLT/' +name +'/'
    df.write.format('delta').mode('overwrite').save(output_path)

# COMMAND ----------

display(df)
