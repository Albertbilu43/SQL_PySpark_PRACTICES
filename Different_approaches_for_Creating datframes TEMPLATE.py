# Databricks notebook source
# MAGIC %md
# MAGIC ## Agenda
# MAGIC
# MAGIC #### 1 Create spark session
# MAGIC
# MAGIC   - installing some libraries
# MAGIC   - Restarting python (in case previous libraries installation asks to do so)
# MAGIC   - Importing some libraries
# MAGIC
# MAGIC #### 2 create dataframe (by reading a file (csv, json, etc.))
# MAGIC
# MAGIC - 1 By reading a file in the Volume(a csv, json, etc.) FOOL(Format, Option, Option, Load)
# MAGIC     -  using  spark.read.format().option().option().load()
# MAGIC - 2 By reading a table where the data sits using spark.read.table
# MAGIC - 3 Check the notebook "3_Customer_Dataframe_Creation".  using spark.createDataFrame(data=data_list, schema=data_schema)
# MAGIC
# MAGIC #### 3 Query the data the SQL Query way and the PySpark Transformation way
# MAGIC   -   SQL Query way
# MAGIC   -   PySpark transformations way
# MAGIC
# MAGIC
# MAGIC #### 4 Example
# MAGIC
# MAGIC --------------------------------------
# MAGIC
# MAGIC ##### Step One: Creating spark session
# MAGIC
# MAGIC  spark.version
# MAGIC
# MAGIC ##### Step Two: Creating datframe
# MAGIC First of all you must create a dataframe and here we list 3 different ways, although we will describe 2 in this notebook, the 3rd one is discussed in a previous nortebook.
# MAGIC
# MAGIC  1 create a Dataframe by reading a file in the Volume(a csv, json, etc.)
# MAGIC
# MAGIC -     using spark.read.format
# MAGIC                       .option
# MAGIC                       .option
# MAGIC                       .load
# MAGIC
# MAGIC 2 Create a dataframe by reading a table where the data sits
# MAGIC
# MAGIC -     using spark.read.table
# MAGIC
# MAGIC 3 Check the notebook "3_Customer_Dataframe_Creation". We are not reviewing this one for this practice
# MAGIC
# MAGIC -     using spark.createDataFrame(data=data_list, schema=data_schema)
# MAGIC
# MAGIC
# MAGIC ##### Step Three: Query the data SQL Query way and PySpark Transformations way
# MAGIC ------------------------------------------------------------------------------------------
# MAGIC ###### SQL Query way --> Write a normal SQL query 
# MAGIC
# MAGIC ###### PySpark transformations way --> PySpark divides into several steps to perform the steps of an SQL query
# MAGIC
# MAGIC  -    < 1 Read teh data
# MAGIC
# MAGIC  -    < 2 Apply transformations(querying the data)
# MAGIC         - IDEAL :    ENCAPSULATES ALL TRANSFORMATIONS INTO ONE dataframe
# MAGIC         - NOT IDEAL: Creates a dataframe per transformation
# MAGIC
# MAGIC  -    < 3 Show/Execute the result/Actions(applied to teh result)
# MAGIC
# MAGIC ####5 Practices
# MAGIC
# MAGIC ![image_1774032783996.png](./image_1774032783996.png "image_1774032783996.png")
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4 Example

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### 1 Create spark session
# MAGIC

# COMMAND ----------

#This below is a pre-created spark session. It's used when you want t avoid creating the spark session with teh builder. 
# We will use this to create a spark session from here onwards
#spark.version

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.1 Installing some libraries

# COMMAND ----------

#pip install duckdb pandas # install duckdb and pandas to be able to query a dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.2 Restarting python

# COMMAND ----------

#dbutils.library.restartPython() # tHIS restarts the kernel or python after running the above command to install duckdb

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.3 Importing some libraries

# COMMAND ----------

"""
import pandas as pd
import duckdb
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import to_date, col
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### 2 Create dataframe (by reading a file from the Volume(csv, json, etc.))
# MAGIC
# MAGIC ###### Choose one these three below. 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Before starting you can take different approaches about creating the dataframes, you can:
# MAGIC
# MAGIC  <> create a Dataframe by reading a file (in the Volume a csv, json, etc.) 
# MAGIC -     using spark.read.format
# MAGIC                       .option
# MAGIC                       .option
# MAGIC                       .load         
# MAGIC
# MAGIC  <> Create a dataframe by reading a table where the data sits  
# MAGIC -     using spark.read.table
# MAGIC
# MAGIC  <> Check the notebook "3_Customer_Dataframe_Creation". We are not reviewing this one for this practice
# MAGIC     
# MAGIC -     df_raw = spark.createDataFrame(data=data_list, schema=data_schema)
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# Examples of How to read a csv and a json file
#  
# Reading a csv file
#file_df = ( spark.read.format('csv')
#                      .option('header', 'true')
#                      .option('inferSchema', 'true')
#                      .load(path="/Volumes/dev/spark_db/datasets/spark_programming/data/sf-fire-calls.csv")
#          )

#Read a json file
#Using a connector(options).
#json_file_df = (
#                spark.read.format('json')
#                .load(path= '/Volumes/dev/spark_db/datasets/spark_programming/data/diamonds.json')
#              )


# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------------------------------------------
# MAGIC ##### <> using spark.read.format

# COMMAND ----------

# Reading a csv file
#df = ( spark.read.format('csv')
#                 .option('header', 'true')
#                 .option('inferSchema', 'true')
#                 .load(path="/Volumes/dev/spark_db/datasets/spark_programming/data/customers.csv")
#     )

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------------------------------------------
# MAGIC ##### <>  using spark.read.table()

# COMMAND ----------

# 1 Read data, table in this case
#cust_df =  spark.read.table("dev.spark_db.customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Let's choose teh first method

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### 3 Query the data SQL Query way and PySpark Transformations way
# MAGIC
# MAGIC ###### Before anything you MUST create a dataframe
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------------------------------------------
# MAGIC -----------------------------------------------------------------------------
# MAGIC #### Request: Query the Top three selling products

# COMMAND ----------

# MAGIC %md
# MAGIC ##### SQL Query 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- SELECT PRODUCTID
# MAGIC -- FROM dev.spark.db.customers
# MAGIC -- ORDER BY quantity desc 
# MAGIC -- LIMIT 3;

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------------------------------------------
# MAGIC
# MAGIC ##### PySpark  divides in 3 steps to perform the above query
# MAGIC
# MAGIC < 1/3 Read teh data
# MAGIC   
# MAGIC -     Creates a df with one of the methods mentioned above
# MAGIC
# MAGIC < 2/3 Apply transformations(querying the data)
# MAGIC
# MAGIC -     Replicate the query the PySpark's way
# MAGIC       - Ideal way (ONE DF <all steps encapsulated>)
# MAGIC       - Not Ideal way (SEVRAL DFs <all steps divided>)
# MAGIC
# MAGIC < 3/3 Show/Execute the result/Actions(applied to teh result)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------------------------------------------
# MAGIC ##### <1/3 Read teh data. A table in this case

# COMMAND ----------

# 1 Read data, table in this case
#cust_df =  spark.read.table("dev.spark_db.customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ------------------------------------------------------------------------------
# MAGIC IDEAL(ONE DF (all steps encapsulated))
# MAGIC ##### <2/3 Apply transformations(Composable query: All steps in encapsualted) 
# MAGIC All transformations are done in one encapsulated query and saved to a new dataframe instead of dividing in datframes)

# COMMAND ----------

"""
from pyspark.sql.functions import expr

result_df = ( cust_df.select("customerid", "customer_name", "productid", "quantity", "price")
                     .where("productid IS NOT NULL")
                     .groupBy("customerid", "customer_name").agg(expr("sum(quantity * price)").alias("sales"))
                     .orderBy("sales", ascending=False)
                     .limit(3)
            )

result_df.display()        

"""

# COMMAND ----------

# MAGIC %md
# MAGIC ---------------------------------------------------------------------
# MAGIC NOT IDEAL (SEVERAL DFs (all steps divided))
# MAGIC ##### <2/3 Apply transformations(Divide into steps). This is other way but reqires the creation of too many dataframes
# MAGIC
# MAGIC Dividing the query in STEPS, ONE dataframe for EACH transformation
# MAGIC

# COMMAND ----------

# The aggreations are not done in the SELECT , they are done in the GROUPBY
# expr() operates on a single SQL expression and returns a Column object for use within other DataFrame methods.
#  This function is useful when you want to apply a SINGLE SQL expression to a DataFrame column or perform a SQL-like operation on a DataFrame

# select  Returns a DataFrame with subset (or all) of columns.

# COMMAND ----------

# DBTITLE 1,PySpark top two selling products transformation
#from pyspark.sql.functions import expr

#df1 = cust_df.select("customerid", "customer_name", "productid", "quantity", "price")
#df2 = df1.where("productid IS NOT NULL")
#df3 = df2.groupBy("customerid", "customer_name").agg(expr("sum(quantity * price)").alias("sales"))
#df4= df3.orderBy("sales", ascending=False)
#df5= df4.limit(3)
#df5.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### 5 PRACTICES 

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Start spark session

# COMMAND ----------

###### Start spark session
spark.version


# COMMAND ----------

# MAGIC %md
# MAGIC ###### Pip install libraries

# COMMAND ----------

pip install duckdb pandas # install duckdb and pandas to be able to query a dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Restart python to catch libraries updates

# COMMAND ----------


dbutils.library.restartPython() #  Restarts the kernel or python after running the above command to install duckdb

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Import libraries

# COMMAND ----------

###### Import libraries
import pandas as pd
import duckdb
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import to_date, col


# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------
# MAGIC ###### SQL query request: Count Disctinct Productids
# MAGIC --------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ###### SQL query
# MAGIC ----------------------------------------------------------------------

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT PRODUCTID) AS DISTINCT_PRODUCT_COUNT 
# MAGIC FROM dev.spark_db.customers

# COMMAND ----------

# MAGIC %md
# MAGIC ###### PySpark divides in three steps to perform the above query
# MAGIC ----------------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Reading csv file to create DataFrame

# COMMAND ----------

# Create dataframe by reading a csv file
df = ( spark.read.format('csv')
                 .option('header', 'true')
                 .option('inferSchema', 'true')
                 .load(path="/Volumes/dev/spark_db/datasets/spark_programming/data/customers.csv")
     )

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Pyspark transformation of the SQL query
# MAGIC ######Take the same piece of the SQL query above: COUNT(DISTINCT PRODUCTID) AS DISTINCT_PRODUCT_COUNT 
# MAGIC
# MAGIC ######and, in pyspark, put into the selectExpr() :  --> selectExpr('COUNT(DISTINCT PRODUCTID) AS DISTINCT_PRODUCT_COUNT ')
# MAGIC
# MAGIC
# MAGIC  

# COMMAND ----------

res_df2 = (df.selectExpr('count(distinct productid) as distinct_product_count') 
              )
res_df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ----------------------------------------------------------------------
# MAGIC ###### Request: Distinct customers 
# MAGIC ----------------------------------------------------------------------
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###### SQL query

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct customerid
# MAGIC from dev.spark_db.customers

# COMMAND ----------

# MAGIC %md
# MAGIC ###### PySpark divides in three steps to perform the above query
# MAGIC ----------------------------------------------------------------------
# MAGIC
# MAGIC ###### Take the same piece of the SQL query above: distinct customerid 
# MAGIC
# MAGIC ###### and, in pyspark, put into the selectExpr() :  --> selectExpr('distinct customerid')

# COMMAND ----------

# DBTITLE 1,Distinct customers PySpark fix
res_df3 = (df.select('customerid').distinct()
                )
res_df3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032783996.png](./image_1774032783996.png "image_1774032783996.png")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## THIS ONE BELOW IS DONE BY USING DuckDb but do not run it. Check later on

# COMMAND ----------

# 1 Read data, table in this case
df2 =  spark.read.table("dev.spark_db.customers")

# COMMAND ----------

# DBTITLE 1,DuckDB query fix: use pandas DataFrame
#import pandas as pd
#import duckdb

#duckdb.register('df2', df2.toPandas())
#query = """
#SELECT distinct customerid
#FROM df2
#"""
#result = duckdb.query(query).df()
#print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC 3 Find duplicates

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")

# COMMAND ----------

