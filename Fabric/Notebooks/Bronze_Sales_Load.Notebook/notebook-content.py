# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "da2d3198-2e02-445f-a9d2-c825c7340adb",
# META       "default_lakehouse_name": "lakehouse",
# META       "default_lakehouse_workspace_id": "4a2509b3-6dd7-45af-8819-5e46b0ca744c",
# META       "known_lakehouses": [
# META         {
# META           "id": "da2d3198-2e02-445f-a9d2-c825c7340adb"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

filepath = "Sales_01012023.xlsx"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

filepath = "abfss://Jumia_Ecommerce_Project_WS@onelake.dfs.fabric.microsoft.com/lakehouse.Lakehouse/Files/Source/" + filepath
filepath

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd



# Read a specific sheet by name
df_sales = pd.read_excel(filepath, sheet_name="Sales")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

filedate = filepath.replace(".xlsx","")[-8:]
filedate

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_sales.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_spark_sales = spark.createDataFrame(df_sales)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_spark_sales.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
from datetime import datetime
df_spark_sales = df_spark_sales.withColumn("LoadTime",lit(datetime.now())).withColumn("FileDate",to_date(lit(filedate),"ddMMyyyy"))
display(df_spark_sales.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%pyspark
# MAGIC 
# MAGIC df_spark_sales.write.format("delta").mode("overwrite").partitionBy("FileDate").saveAsTable("bronze_sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Preparing Customers Data
# 
#  Customer_ID string ,
#  Customer_Name string ,
#  Segment string ,
#  City string ,
#  State string ,
#  Country string ,
#  Region string ,
#  LoadTime timestamp 

# CELL ********************

df_new_customer = df_spark_sales.select(col("Customer_ID"),col("Customer_Name"),col("Segment"),col("City"),col("State"),col("Country"),col("Region"),col("LoadTime"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%pyspark
# MAGIC 
# MAGIC df_new_customer.createOrReplaceTempView("source_customers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC MERGE into dim_customer tgt using (
# MAGIC select *
# MAGIC from source_customers) src on tgt.Customer_ID = src.Customer_ID
# MAGIC WHEN MATCHED THEN UPDATE
# MAGIC SET tgt.LoadTime = src.LoadTime
# MAGIC WHEN not MATCHED then insert (Customer_ID,
# MAGIC Customer_Name,
# MAGIC Segment,
# MAGIC City,
# MAGIC State,
# MAGIC Country,
# MAGIC Region,LoadTime)
# MAGIC values(
# MAGIC src.Customer_ID,
# MAGIC src.Customer_Name,
# MAGIC src.Segment,
# MAGIC src.City,
# MAGIC src.State,
# MAGIC src.Country,
# MAGIC src.Region,src.LoadTime)
# MAGIC ;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
