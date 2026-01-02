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

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC describe  table bronze_sales

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%pyspark
# MAGIC 
# MAGIC df_new_customer = spark.sql('''
# MAGIC select distinct Customer_ID,
# MAGIC Customer_Name,
# MAGIC Segment,
# MAGIC City,
# MAGIC State,
# MAGIC Country,
# MAGIC Region
# MAGIC from bronze_sales ''');

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_new_customer.limit(1))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new_customer.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC create table if not exists dim_customer(
# MAGIC  Customer_ID string ,
# MAGIC  Customer_Name string ,
# MAGIC  Segment string ,
# MAGIC  City string ,
# MAGIC  State string ,
# MAGIC  Country string ,
# MAGIC  Region string ,
# MAGIC  LoadTime timestamp 
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from pyspark.sql.functions import *
from datetime import datetime
df_new_customer = df_new_customer.withColumn("LoadTime",lit(datetime.now()))
display(df_new_customer.limit(10))

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
