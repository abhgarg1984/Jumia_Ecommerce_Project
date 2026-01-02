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

filepath = "abfss://Jumia_Ecommerce_Project_WS@onelake.dfs.fabric.microsoft.com/lakehouse.Lakehouse/Files/Source/"+filepath

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

filepath

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd



# Read a specific sheet by name
df_returns = pd.read_excel(filepath, sheet_name="Returns")



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

display(df_returns.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_spark_returns = spark.createDataFrame(df_returns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_spark_returns.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
from datetime import datetime
df_spark_returns = df_spark_returns.withColumn("LoadTime",lit(datetime.now())).withColumn("FileDate",to_date(lit(filedate),"ddMMyyyy"))
display(df_spark_returns.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%pyspark
# MAGIC 
# MAGIC df_spark_returns.write.format("delta").mode("overwrite").partitionBy("FileDate").saveAsTable("bronze_returns")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
