# Databricks notebook source
# MAGIC %md
# MAGIC ## Agenda
# MAGIC
# MAGIC #### 1 Create spark session
# MAGIC
# MAGIC   - Create spark session
# MAGIC   - installing some libraries
# MAGIC   - Restarting python (in case previous libraries installation asks to do so)
# MAGIC   - Importing some libraries
# MAGIC
# MAGIC #### 2 create dataframe
# MAGIC
# MAGIC -     1 By reading a file in the Volume(a csv, json, etc.) FOOL(Format, Option, Option, Load)
# MAGIC     -  using  spark.read.format().option().option().load()
# MAGIC - 2 By reading a table where the data sits using spark.read.table
# MAGIC - 3 Check the notebook "3_Customer_Dataframe_Creation".  using spark.createDataFrame(data=data_list, schema=data_schema)
# MAGIC
# MAGIC ###### 2.1 Eyeball to spot possible data issues
# MAGIC
# MAGIC ###### 2.2 Fix any issues discovered
# MAGIC
# MAGIC     Fix data types errors with:
# MAGIC
# MAGIC     Single column    --> .withColumn()     col().cast() or use this expr('cast()')
# MAGIC
# MAGIC     Multiple columns --> .withColumns({ }) col().cast() or use this expr('cast()')
# MAGIC
# MAGIC     example: 
# MAGIC        # FIX ONE COLUMN, the order_date as it was incorrectly inferred
# MAGIC        df_fixed2 =  df2_raw.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")  ) 
# MAGIC
# MAGIC        # FIX MORE THAN ONE COLUMN at the same time
# MAGIC        #-------------------OPTION ONE --Using col()----------------
# MAGIC        #df_fixed2 =  df2_raw.withColumns({"order_date": to_date(col("order_date"), "yyyy-MM-dd"),
# MAGIC        #                                  "order_id": col("order_id").cast("string")
# MAGIC        #                               #"order_id": col("order_id").try_cast(IntegerType()) try_cast returns null if an error hapens    
# MAGIC        #                                 })
# MAGIC
# MAGIC        #-------------------OPTION TWO --Using expr()----------------
# MAGIC        #df_fixed2 =  df2_raw.withColumns({"order_date": to_date(col("order_date"), "yyyy-MM-dd"),
# MAGIC        #                                  "order_id": expr("cast(order_id as string)")    
# MAGIC        #                                 })
# MAGIC
# MAGIC
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
# MAGIC ###### *   SQL Query way --> Write a normal SQL query 
# MAGIC
# MAGIC ###### *   PySpark transformations way --> IDEAL to perform the steps of an SQL query
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
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032783996.png](./image_1774032783996.png "image_1774032783996.png")
# MAGIC #### 1 Create spark session
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating spark session

# COMMAND ----------

# We will use this to create a spark session from here onwards
spark.version

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### installing some libraries

# COMMAND ----------

pip install duckdb pandas # install duckdb and pandas to be able to query a dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------
# MAGIC #####Restarting python 

# COMMAND ----------

dbutils.library.restartPython() # tHIS restarts the kernel or python after running the above command to install duckdb

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------
# MAGIC #####Importing some libraries

# COMMAND ----------

import pandas as pd
import duckdb
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import to_date, col, expr

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC ### 2 create dataframe 
# MAGIC
# MAGIC ##### (by reading a file from Volume (csv, json, etc.) )
# MAGIC

# COMMAND ----------

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
# MAGIC ##### <> using spark.read.format   (FOOL)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC  <> create a Dataframe by reading a file (in the Volume a csv, json, etc.) 
# MAGIC -     using spark.read.format
# MAGIC                       .option
# MAGIC                       .option
# MAGIC                       .load         

# COMMAND ----------


raw_cust_df = ( spark.read.format('csv')
                          .option("header", True)
                          .option("inferSchema", True)
                          .load("/Volumes/dev/spark_db/datasets/spark_programmingHBL/data/customers.csv")
              )

#raw_cust_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### 2.1 Eyeball the data to spot possible issues

# COMMAND ----------

# MAGIC %md
# MAGIC ------------------------------------------------------------------
# MAGIC #####2.2 Fix any data issues discovered.
# MAGIC After reading the data we noticed that no issues so no actions needed

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC  In case you need to make fixes you can make use of these : 
# MAGIC
# MAGIC  From PySpark use either withColumn() or withColumns()
# MAGIC
# MAGIC ##### 1 withColumn() to add a SINGLE column or replacing the existing column that has the same name. 
# MAGIC
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumn.html
# MAGIC
# MAGIC ##### 2 withColumns() to add MULTIPLE columns or replacing the existing columns that have the same names.
# MAGIC
# MAGIC doc: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.html
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC ### 3 Query  the data the SQL Query way and the PySpark Transformation way
# MAGIC
# MAGIC ##### Before anything you MUST create a dataframe
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032783996.png](./image_1774032783996.png "image_1774032783996.png")
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC
# MAGIC #### Top three selling products
# MAGIC ------------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT productid, sum(quantity * price) as sales
# MAGIC FROM   dev.spark_db.customers as c
# MAGIC Where  productid IS NOT NULL
# MAGIC group by productid
# MAGIC order by sales desc
# MAGIC limit 3
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ------------------------------------------------------------------
# MAGIC ##### PySpark
# MAGIC
# MAGIC ![image_1774556836253.png](./image_1774556836253.png "image_1774556836253.png")
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###### 1 Read teh data.

# COMMAND ----------

# 1 This steps is done when we created the dataframe by reading a file from Volume "raw_cust_df"

# COMMAND ----------

# MAGIC %md
# MAGIC ###### 2 Apply transformations(Composable query: All steps  encapsualted) 
# MAGIC
# MAGIC ##### Now you have to use the newly created dataframe "raw_cust_df"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #####Remember: If using select() and selectExpr() 
# MAGIC   - select() uses the DataFrame API's column objects and functions
# MAGIC   - selectExpr() accepts SQL-style expressions as strings.
# MAGIC     -     But be careful when using groupBy() and agg(). See the notebook "Pyspark Tips1"
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###STANDARD WAY
# MAGIC ##### The piece of code below is the STANDARD WAY of performing 
# MAGIC - First the groupBy 
# MAGIC - Then the  aggregations 

# COMMAND ----------

from pyspark.sql.functions import to_date, col, expr, sum, count


result_df =(  raw_cust_df.groupBy("productid")
                         .agg(expr("sum(quantity * price)  as Revenue"))
                         .orderBy("Revenue", ascending=False)
                         .limit(3)
           )
            
result_df.display()



# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------------
# MAGIC ##### Using .select()

# COMMAND ----------


from pyspark.sql.functions import expr

cust_df= raw_cust_df

result_df = ( cust_df.select("customerid", "customer_name", "productid", "quantity", "price")
                     .where("productid IS NOT NULL")
                     .groupBy("productid")
                     .agg(expr("sum(quantity * price)").alias("sales"))
                     .orderBy("sales", ascending=False)
                     .limit(3)
            )

result_df.display()            


# COMMAND ----------

# MAGIC %md
# MAGIC ##error:
# MAGIC ##### This piece of code below will produce an error cause 
# MAGIC
# MAGIC you cannot replicate the query directly using a selectExper() as it requires the registration of the dataframe as a temporary view first
# MAGIC
# MAGIC
# MAGIC Use the standard way instead

# COMMAND ----------

""" selectExpr() example
from pyspark.sql.functions import to_date, col, expr, sum, count

result_df =(  raw_cust_df.selectExpr("productid" ,"sum(quantity * price) as Revenue")
                         .orderBy("Revenue", ascending=False)
                         .limit(3)
           )
            
result_df.display()
"""       

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC ### SOME PRACTICES BELOW
# MAGIC
# MAGIC #### 10-50 SQL Interview Questions 
# MAGIC
# MAGIC https://www.linkedin.com/feed/update/urn:li:activity:7402591561454383104/?updateEntityUrn=urn%3Ali%3Afs_updateV2%3A%28urn%3Ali%3Aactivity%3A7402591561454383104%2CFEED_DETAIL%2CEMPTY%2CDEFAULT%2Cfalse%29
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### CUSTOMERS

# COMMAND ----------

raw_cust_df = ( spark.read.format('csv')
                          .option("header", True)
                          .option("inferSchema", True)
                          .load("/Volumes/dev/spark_db/datasets/spark_programmingHBL/data/customers.csv")
              )

#raw_cust_df.display()



# COMMAND ----------

# MAGIC %md
# MAGIC ### ORDERS

# COMMAND ----------

raw_ord_df = ( spark.read.format('csv')
                          .option("header", True)
                          .option("inferSchema", True)
                          .load("/Volumes/dev/spark_db/datasets/spark_programmingHBL/data/orders.csv")
              )

#raw_cust_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### PRODUCTS

# COMMAND ----------

raw_prod_df = ( spark.read.format('csv')
                          .option("header", True)
                          .option("inferSchema", True)
                          .load("/Volumes/dev/spark_db/datasets/spark_programmingHBL/data/products.csv")
              )

#raw_cust_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### RETURNS

# COMMAND ----------

raw_ret_df = ( spark.read.format('csv')
                          .option("header", True)
                          .option("inferSchema", True)
                          .load("/Volumes/dev/spark_db/datasets/spark_programmingHBL/data/returns.csv")
              )

#raw_ret_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1 Find duplicates
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT customerid, count(*)
# MAGIC FROM dev.spark_db.customers as C
# MAGIC GROUP BY customerid
# MAGIC HAVING count(*) > 1

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark
# MAGIC
# MAGIC Method 1: Using DataFrame API (Recommended)
# MAGIC
# MAGIC Method 2: Using a TemporaryView and Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 1: Using DataFrame API (Recommended)
# MAGIC This approach is the most common and "PySpark way" to achieve the result.

# COMMAND ----------

from pyspark.sql.functions import col, count, expr

grouped_df = ( raw_cust_df.groupBy("customerid")
                          .agg(expr("count(*) as customerids") )
                          .filter("customerids > 1") # This works as the 'having' filter for group by in SQL
             )

grouped_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Other ways

# COMMAND ----------

from pyspark.sql.functions import col, count


# 1. Group by 'customerid' and count the occurrences, giving the count column an alias
#    (e.g., 'customer_count')
grouped_df = (cust_df.groupBy("customerid") \
                     .agg(count("*").alias("customer_count")) #
             )
# 2. Filter the result to keep only rows where the count is greater than 1
result_df = grouped_df.filter(col("customer_count") > 1) #

# 3. Show the result
result_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 2: Using a TemporaryView and Spark SQL
# MAGIC
# MAGIC You can also use raw SQL directly within PySpark by registering your DataFrame as a temporary view.

# COMMAND ----------

 
 

# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame.
# Basically the "raw_cust_df" dataframe was built, using the FOOL format, from the customers.csv file 
raw_cust_df.createOrReplaceTempView("customers") # We give the view  a name 'customers'

# Run the SQL query using spark.sql()
sql_query = """
SELECT customerid, count(*) as customer_count
FROM customers
GROUP BY customerid
HAVING count(*) > 1
"""
result_df_sql = spark.sql(sql_query)

# Show the result
result_df_sql.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ##### remove dupllicates
# MAGIC
# MAGIC df.dropDuplicates(["id", "source", "destination"]).display()

# COMMAND ----------

raw_cust_df.dropDuplicates(["customerid", "departmentid"]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### Calculate total revenue per product
# MAGIC
# MAGIC -------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ##### SQL query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT product_name
# MAGIC       ,sum(quantity * price) as product_revenue 
# MAGIC       ,(sum(quantity * price) * 100.0 ) / (select sum(quantity * price) from dev.spark_db.customers) AS percent 
# MAGIC FROM dev.spark_db.customers as C
# MAGIC GROUP BY product_name
# MAGIC ;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark
# MAGIC
# MAGIC Method 1: Using DataFrame API (Recommended)
# MAGIC
# MAGIC Method 2: Using a TemporaryView and Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 1: Using DataFrame API (Recommended)
# MAGIC This approach is the most common  "PySpark way" to achieve the result.

# COMMAND ----------

from pyspark.sql.functions import expr, col, count, sum

# Get the toal revenue
#total_revenue = raw_cust_df.selectExpr('sum(quantity * price) as total_revenue').collect()[0]['total_revenue']
#total_revenue = raw_cust_df.agg(expr('sum(quantity * price) as total_revenue')).first()[0]
total_revenue = raw_cust_df.agg( sum(col('quantity') * col('price') )).first()[0]

result_df = ( raw_cust_df.groupBy("product_name")
                         .agg(  expr("sum(quantity * price) as product_revenue") 
                              ,(expr("sum(quantity * price) * 100.0") / total_revenue).alias("percent")
                             )
            )

result_df.display()   

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 2: Using a TemporaryView and Spark SQL
# MAGIC
# MAGIC You can also use raw SQL directly within PySpark by registering your DataFrame as a temporary view.

# COMMAND ----------


# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame.
# Basically the "raw_cust_df" dataframe was built, using the FOOL format, from the customers.csv file 
raw_cust_df.createOrReplaceTempView('customers') # We give the view  a name 'customers'

# Run the SQL query using spark.sql()
sql_query = """
SELECT product_name
      ,sum(quantity * price) as product_revenue 
      ,(sum(quantity * price) * 100.0 ) / (select sum(quantity * price) from customers) AS percent 
FROM customers as C
GROUP BY product_name
"""
result_df = spark.sql(sql_query)
result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### Customers Who made Purchases but never returned products
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT C.customerid
# MAGIC FROM      dev.spark_db.customers  as C
# MAGIC LEFT JOIN dev.spark_db.returns    as R on C.customerid = R.customerid
# MAGIC WHERE R.customerid IS NULL
# MAGIC
# MAGIC
# MAGIC --SELECT DISTINCT C.customerid
# MAGIC --FROM      dev.spark_db.customers  as C
# MAGIC --LEFT JOIN dev.spark_db.returns    as R on C.customerid = R.customerid
# MAGIC --WHERE C.customerid NOT IN (SELECT R.customerid FROM dev.spark_db.returns R)
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark
# MAGIC
# MAGIC Method 1: Using DataFrame API (Recommended)
# MAGIC
# MAGIC Method 2: Using a TemporaryView and Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)
# MAGIC
# MAGIC 3. Handling Duplicate ColumnsWhen using a boolean expression (like df1.id == df2.id), both columns will appear in the result, which can cause "ambiguous column" errors later.Fix: 
# MAGIC -         Use alias: A better approach is to assign aliases to the dataframes, and then reference the output columns from the join operation using these aliases:
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import expr, col, count


joined_df= (raw_cust_df.alias('C').join(raw_ret_df.alias('R'), col("C.customerid") == col("R.customerid"), how="left")
              .filter(col("R.customerid").isNull())
              .select("C.customerid")
              .distinct()
            )

joined_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------


# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame.
# Basically the "raw_cust_df" dataframe was built, using the FOOL format, from the customers.csv file 
raw_cust_df.createOrReplaceTempView('customers') # We give the view  a name 'customers'
raw_ret_df.createOrReplaceTempView('returns') # We give the view  a name 'returns'

# Run the SQL query using spark.sql()

sql_query="""
SELECT DISTINCT C.customerid
FROM      customers  as C
LEFT JOIN returns    as R on C.customerid = R.customerid
WHERE R.customerid IS NULL
"""

result_query =  spark.sql(sql_query)
result_query.display()



# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### Show the count of orderds by customers
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT C.customerid, count(c.order_id) AS order_count
# MAGIC FROM      dev.spark_db.customers  as C
# MAGIC group by C.customerid

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark
# MAGIC
# MAGIC Method 1: Using DataFrame API (Recommended)
# MAGIC
# MAGIC Method 2: Using a TemporaryView and Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)

# COMMAND ----------

from pyspark.sql.functions import expr, col, count

new_df= (raw_cust_df.groupBy("customerid")
                    .agg(count("order_id").alias("order_count"))
                    .distinct()
            )

new_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------

# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame.
# Basically the "raw_cust_df" dataframe was built, using the FOOL format, from the customers.csv file 
raw_cust_df.createOrReplaceTempView('customers') # We give the view  a name 'customers'
#raw_ret_df.createOrReplaceTempView('returns') # We give the view  a name 'returns'

# Run the SQL query using spark.sql()

sql_query="""
SELECT DISTINCT C.customerid, count(c.order_id) AS order_count
FROM     customers  as C
group by C.customerid
"""

result_query =  spark.sql(sql_query)
result_query.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### Calculate avg order value per customer
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH CTE AS ( SELECT CUSTOMERID, order_id, SUM(quantity * price) AS order_value
# MAGIC               FROM  dev.spark_db.customers
# MAGIC               GROUP BY CUSTOMERID, order_id
# MAGIC             ) 
# MAGIC SELECT CUSTOMERID, AVG(ORDER_VALUE) AS AVG_ORDER_VALUE
# MAGIC FROM CTE
# MAGIC GROUP BY CUSTOMERID; 

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark
# MAGIC
# MAGIC Method 1: Using DataFrame API (Recommended)
# MAGIC
# MAGIC Method 2: Using a TemporaryView and Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------

# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame.
# Basically the "raw_cust_df" dataframe was built, using the FOOL format, from the customers.csv file 
raw_cust_df.createOrReplaceTempView('customers') # We give the view  a name 'customers'
#raw_ret_df.createOrReplaceTempView('returns') # We give the view  a name 'returns'

# Run the SQL query using spark.sql()

sql_query="""
WITH CTE AS ( SELECT CUSTOMERID, order_id, SUM(quantity * price) AS order_value
              FROM  dev.spark_db.customers
              GROUP BY CUSTOMERID, order_id
            ) 
SELECT CUSTOMERID, AVG(ORDER_VALUE) AS AVG_ORDER_VALUE
FROM CTE
GROUP BY CUSTOMERID
"""

result_query =  spark.sql(sql_query)
result_query.display()


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### Get the latest order placed per customer
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CUSTOMERID, Max(order_date) AS LAST_ORDER_DATE
# MAGIC               FROM  dev.spark_db.customers
# MAGIC               GROUP BY CUSTOMERID;

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark
# MAGIC
# MAGIC Method 1: Using DataFrame API (Recommended)
# MAGIC
# MAGIC Method 2: Using a TemporaryView and Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)

# COMMAND ----------

from pyspark.sql.functions import expr, col, count, max

result_df= (raw_cust_df.groupBy("customerid")
                       .agg(max("order_date").alias("last_order_date"))
            )

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------


# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame.
# Basically the "raw_cust_df" dataframe was built, using the FOOL format, from the customers.csv file 
raw_cust_df.createOrReplaceTempView('customers') # We give the view  a name 'customers'
#raw_ret_df.createOrReplaceTempView('returns') # We give the view  a name 'returns'

# Run the SQL query using spark.sql()

sql_query="""
SELECT CUSTOMERID, Max(order_date) AS LAST_ORDER_DATE
              FROM customers
              GROUP BY CUSTOMERID;
"""

result_query =  spark.sql(sql_query)
result_query.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### Find products that were never sold
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC               SELECT P.productid, P.product_name
# MAGIC               FROM  dev.spark_db.products as P
# MAGIC               LEFT JOIN dev.spark_db.orders as O on P.productid=O.productid
# MAGIC               WHERE O.productid IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark
# MAGIC
# MAGIC Method 1: Using DataFrame API (Recommended)
# MAGIC
# MAGIC Method 2: Using a TemporaryView and Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)

# COMMAND ----------

from pyspark.sql.functions import expr, col, count, max

result_df =  ( raw_prod_df.alias('P').join(raw_ord_df.alias('O'), col("P.productid") == col("O.productid"), how='Left')
                           .where(col('O.productid').isNull())
                           .select("P.productid", "P.product_name")
                           .distinct()

             )

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------



# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame.
# Basically the "raw_cust_df" dataframe was built, using the FOOL format, from the customers.csv file 
raw_prod_df.createOrReplaceTempView('products') # We give the view  a name 'products'
raw_ord_df.createOrReplaceTempView('orders')   # We give the view  a name 'orders'

# Run the SQL query using spark.sql()
sql_query="""
 SELECT P.productid, P.product_name
FROM  products as P
LEFT JOIN orders as O on P.productid=O.productid
 WHERE O.productid IS NULL;
"""

result_df =  spark.sql(sql_query)
result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### Most Selling Product
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC               SELECT P.productid, P.product_name, SUM(quantity) AS total_quantity
# MAGIC               FROM  dev.spark_db.customers as P
# MAGIC               GROUP BY P.productid, P.product_name
# MAGIC               ORDER BY total_quantity DESC
# MAGIC               LIMIT 1
# MAGIC               ;

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark
# MAGIC
# MAGIC Method 1: Using DataFrame API (Recommended)
# MAGIC
# MAGIC Method 2: Using a TemporaryView and Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)

# COMMAND ----------

from pyspark.sql.functions import expr, col, count, max, sum
result_df =  ( raw_cust_df.groupBy("productid", "product_name")
                          .agg(sum("quantity").alias("total_quantity"))
                          .orderBy(col("total_quantity").desc())
                          .limit(1)

             )

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------


# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame.
# Basically the "raw_cust_df" dataframe was built, using the FOOL format, from the customers.csv file 
raw_cust_df.createOrReplaceTempView('customers') # We give the view  a name 'customers'

# Run the SQL query using spark.sql()
sql_query="""
              SELECT P.productid, P.product_name, SUM(quantity) AS total_quantity
              FROM  customers as P
              GROUP BY P.productid, P.product_name
              ORDER BY total_quantity DESC
              LIMIT 1
              ;
"""
result_df= spark.sql(sql_query)
result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### Total revenue and Number of orders per region 
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC               SELECT  region, SUM( quantity * price) AS total_revenue, COUNT(P.ORDER_ID) AS order_count
# MAGIC               FROM  dev.spark_db.customers AS P
# MAGIC               join  dev.spark_db.orders    AS O ON P.ORDER_ID=O.ORDER_ID
# MAGIC               GROUP BY region
# MAGIC               ORDER BY total_revenue DESC
# MAGIC               ;

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark
# MAGIC
# MAGIC Method 1: Using DataFrame API (Recommended)
# MAGIC
# MAGIC Method 2: Using a TemporaryView and Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)

# COMMAND ----------

from pyspark.sql.functions import expr, col, count, max, sum

result_df =  ( raw_cust_df.alias('C').join(raw_ord_df.alias('O'), col("C.ORDER_ID") == col("O.ORDER_ID"), how="inner")
                                     .groupBy("region")
                                     .agg( sum(expr("quantity * price")).alias("total_revenue"), count(col("C.ORDER_ID")).alias("order_count"))
                                     .orderBy(col("total_revenue").desc() )

             )

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------


# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame.
# Basically the "raw_cust_df" dataframe was built, using the FOOL format, from the customers.csv file 
raw_cust_df.createOrReplaceTempView('customers') # We give the view  a name 'customers'
raw_ord_df.createOrReplaceTempView('orders')   # We give the view  a name 'orders'

# Run the SQL query using spark.sql()
sql_query="""
              SELECT  region, SUM( quantity * price) AS total_revenue, COUNT(P.ORDER_ID) AS order_count
              FROM  customers AS P
              join  orders    AS O ON P.ORDER_ID=O.ORDER_ID
              GROUP BY region
              ORDER BY total_revenue DESC
              ;
"""

result_df =  spark.sql(sql_query)
result_df.display()



# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### Count customers with more than 5 orders
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC               SELECT  customerid,  COUNT(C.ORDER_ID) AS order_count
# MAGIC               FROM  dev.spark_db.customers AS C
# MAGIC               GROUP BY customerid
# MAGIC               HAVING order_count >5
# MAGIC               ;

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark
# MAGIC
# MAGIC Method 1: Using DataFrame API (Recommended)
# MAGIC
# MAGIC Method 2: Using a TemporaryView and Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)

# COMMAND ----------

from pyspark.sql.functions import expr, col, count, max, sum

result_df =  ( raw_cust_df.groupBy("customerid")
                          .agg(count("order_id").alias("order_count"))
                          .filter(col("order_count") > 5)                 # Rember that  where/filter is used instead of 'having' 
             )

result_df.display()             

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------

# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame.
# Basically the "raw_cust_df" dataframe was built, using the FOOL format, from the customers.csv file 
raw_cust_df.createOrReplaceTempView('customers') # We give the view  a name 'customers'
#raw_ord_df.createOrReplaceTempView('orders')   # We give the view  a name 'orders'

# Run the SQL query using spark.sql()
sql_query="""
              SELECT  customerid,  COUNT(C.ORDER_ID) AS order_count
              FROM  customers AS C
              GROUP BY customerid
              HAVING order_count >5
              ;
"""

result_df =  spark.sql(sql_query)
result_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### Customers with orders above the avg order value 
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH CTE AS (  SELECT  CUSTOMERID, ORDER_ID, SUM(QUANTITY * PRICE) AS ORDER_VALUE
# MAGIC                FROM   dev.spark_db.customers    AS C 
# MAGIC                GROUP BY CUSTOMERID, ORDER_ID
# MAGIC             ) 
# MAGIC
# MAGIC SELECT CUSTOMERID
# MAGIC       ,AVG(ORDER_VALUE)
# MAGIC       ,(SELECT AVG(ORDER_VALUE) FROM CTE) AS AVG_ORDER_VALUE
# MAGIC FROM CTE
# MAGIC WHERE ORDER_VALUE > (SELECT AVG(ORDER_VALUE) FROM CTE)
# MAGIC GROUP BY  CUSTOMERID
# MAGIC ;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark
# MAGIC
# MAGIC Method 1: Using DataFrame API (Recommended)
# MAGIC
# MAGIC Method 2: Using a TemporaryView and Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------


# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame.
# Basically the "raw_cust_df" dataframe was built, using the FOOL format, from the customers.csv file 
raw_cust_df.createOrReplaceTempView('customers') # We give the view  a name 'customers'
#raw_ord_df.createOrReplaceTempView('orders')   # We give the view  a name 'orders'

# Run the SQL query using spark.sql()
sql_query="""
WITH CTE AS (  SELECT  CUSTOMERID, ORDER_ID, SUM(QUANTITY * PRICE) AS ORDER_VALUE
               FROM   customers    AS C 
               GROUP BY CUSTOMERID, ORDER_ID
            ) 

SELECT CUSTOMERID
      ,AVG(ORDER_VALUE)
      ,(SELECT AVG(ORDER_VALUE) FROM CTE) AS AVG_ORDER_VALUE
FROM CTE
WHERE ORDER_VALUE > (SELECT AVG(ORDER_VALUE) FROM CTE)
GROUP BY  CUSTOMERID
;
"""

result_df =  spark.sql(sql_query)
result_df.display()




# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### Get Monthly sales revenue and order count
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC                SELECT  extract(month from order_date) AS month, 
# MAGIC                        SUM(quantity * price) AS revenue, 
# MAGIC                        COUNT(order_id) AS order_count
# MAGIC                FROM   dev.spark_db.customers    AS C
# MAGIC                GROUP BY extract(month from order_date)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark
# MAGIC
# MAGIC Method 1: Using DataFrame API (Recommended)
# MAGIC
# MAGIC Method 2: Using a TemporaryView and Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)

# COMMAND ----------

from pyspark.sql.functions import expr, col, count, sum

result_df = (raw_cust_df.groupBy(expr("EXTRACT(MONTH FROM order_date) AS month"))
                        .agg(sum(expr("quantity * price")).alias("revenue"),count("order_id").alias("order_count") )
            )

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------


# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame.
# Basically the "raw_cust_df" dataframe was built, using the FOOL format, from the customers.csv file 
raw_cust_df.createOrReplaceTempView('customers') # We give the view  a name 'customers'
#raw_ord_df.createOrReplaceTempView('orders')   # We give the view  a name 'orders'

# Run the SQL query using spark.sql()
sql_query="""
               SELECT  extract(month from order_date) AS month, 
                       SUM(quantity * price) AS revenue, 
                       COUNT(order_id) AS order_count
               FROM   customers    AS C
               GROUP BY extract(month from order_date)
;
"""

result_df =  spark.sql(sql_query)
result_df.display()




# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### Find customers who placed orders every month in 2024
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH CTE AS (
# MAGIC               SELECT   CUSTOMERID
# MAGIC                        ,EXTRACT(MONTH FROM ORDER_DATE) AS MONTH
# MAGIC               FROM   dev.spark_db.customers    AS C 
# MAGIC               WHERE EXTRACT(YEAR FROM ORDER_DATE) = 2024
# MAGIC             )
# MAGIC SELECT CUSTOMERID, MONTH
# MAGIC FROM CTE
# MAGIC GROUP BY CUSTOMERID, MONTH
# MAGIC HAVING SUM(DISTINCT MONTH)= 12
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC
# MAGIC /*WITH CTE AS (
# MAGIC                SELECT  customerid,order_date
# MAGIC                       , extract(month from order_date) AS month
# MAGIC                       ,LAG(extract(month from order_date), 1, 0) OVER (PARTITION BY customerid ORDER BY order_date) AS prev_month
# MAGIC
# MAGIC                FROM   dev.spark_db.customers    AS C
# MAGIC                WHERE EXTRACT(YEAR FROM order_date) = 2024 
# MAGIC                GROUP BY customerid,order_date,  extract(month from order_date)
# MAGIC ) 
# MAGIC SELECT *
# MAGIC FROM CTE
# MAGIC WHERE prev_month = month - 1;
# MAGIC
# MAGIC */
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------


# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame.
# Basically the "raw_cust_df" dataframe was built, using the FOOL format, from the customers.csv file 
raw_cust_df.createOrReplaceTempView('customers') # We give the view  a name 'customers'
#raw_ord_df.createOrReplaceTempView('orders')   # We give the view  a name 'orders'

# Run the SQL query using spark.sql()
sql_query="""
WITH CTE AS (
              SELECT   CUSTOMERID
                       ,EXTRACT(MONTH FROM ORDER_DATE) AS MONTH
              FROM   dev.spark_db.customers    AS C 
              WHERE EXTRACT(YEAR FROM ORDER_DATE) = 2024
            )
SELECT CUSTOMERID, MONTH
FROM CTE
GROUP BY CUSTOMERID, MONTH
HAVING SUM(DISTINCT MONTH)= 12
;
"""

result_df =  spark.sql(sql_query)
result_df.display()




# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### Find moving average os sales over the last 3 days
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC                SELECT ORDER_DATE, SUM(quantity * price) as sales
# MAGIC                      ,AVG(SUM(quantity * price)) OVER(ORDER BY order_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg_sales                     
# MAGIC                FROM   dev.spark_db.customers    AS C
# MAGIC                GROUP BY ORDER_DATE
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark
# MAGIC
# MAGIC Method 1: Using DataFrame API (Recommended)
# MAGIC
# MAGIC Method 2: Using a TemporaryView and Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)

# COMMAND ----------

from pyspark.sql.functions import expr, col, count, max, sum, avg

result_df =  ( raw_cust_df.groupBy("order_date")
                          .agg( sum(col("quantity") * col("price")).alias("sales") )
                          .withColumn("Moving_average_sales", expr("AVG(sales) OVER (ORDER BY order_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)"))
                      
             )

result_df.display()


# <<<<<<<<<<<< This code works but it gives a warning on a partition!!! >>>>>>>
#from pyspark.sql.functions import expr, col, count, max, sum, avg
#from pyspark.sql.window import Window
#window_spec= Window.orderBy("order_date").rowsBetween(-2, 0)
#result_df =  ( raw_cust_df.groupBy("order_date")
#                            .agg( sum(col("quantity") * col("price")).alias("sales") )
#                         .withColumn("Moving_average_sales", avg("sales").over(window_spec))
#             )

#result_df.display()



# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------


# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame.
# Basically the "raw_cust_df" dataframe was built, using the FOOL format, from the customers.csv file 
raw_cust_df.createOrReplaceTempView('customers') # We give the view  a name 'customers'
#raw_ord_df.createOrReplaceTempView('orders')   # We give the view  a name 'orders'

# Run the SQL query using spark.sql()
sql_query="""
               SELECT ORDER_DATE, SUM(quantity * price) as sales
                     ,AVG(SUM(quantity * price)) OVER(ORDER BY order_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg_sales                     
               FROM   customers    AS C
               GROUP BY ORDER_DATE
;
"""

result_df =  spark.sql(sql_query)
result_df.display()




# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### Identify the first and last order date for each customer
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC                SELECT customerid, MIN(ORDER_DATE) AS FIRST_ORDER , MAX(ORDER_DATE) AS LAST_ORDER
# MAGIC                FROM   dev.spark_db.customers    AS C
# MAGIC                GROUP BY customerid

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark
# MAGIC
# MAGIC Method 1: Using DataFrame API (Recommended)
# MAGIC
# MAGIC Method 2: Using a TemporaryView and Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)

# COMMAND ----------

from pyspark.sql.functions import expr, col, count, min, max, sum, avg
from pyspark.sql.window import Window

result_df =  ( raw_cust_df.groupBy("customerid")
                          .agg(expr("MIN(order_date) AS first_order"), expr("MAX(order_date) AS last_order") )
             )
result_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------


# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame.
# Basically the "raw_cust_df" dataframe was built, using the FOOL format, from the customers.csv file 
raw_cust_df.createOrReplaceTempView('customers') # We give the view  a name 'customers'
#raw_ord_df.createOrReplaceTempView('orders')   # We give the view  a name 'orders'

# Run the SQL query using spark.sql()
sql_query="""
               SELECT customerid, MIN(ORDER_DATE) AS FIRST_ORDER , MAX(ORDER_DATE) AS LAST_ORDER
               FROM  customers    AS C
               GROUP BY customerid
"""

result_df =  spark.sql(sql_query)
result_df.display()



# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### Show product sales distribution (percent of toal revenue)
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC                SELECT productid 
# MAGIC                      ,sum(quantity * price) AS product_revenue
# MAGIC                      ,sum(quantity * price) * 100.0 / (SELECT sum(quantity * price) FROM dev.spark_db.customers) AS percent
# MAGIC                FROM   dev.spark_db.customers    AS C
# MAGIC                GROUP BY productid
# MAGIC
# MAGIC ;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark
# MAGIC
# MAGIC Method 1: Using DataFrame API (Recommended)
# MAGIC
# MAGIC Method 2: Using a TemporaryView and Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)

# COMMAND ----------

from pyspark.sql.functions import expr, col, count, min, max, sum, avg
#from pyspark.sql.window import Window

total_revenue = raw_cust_df.select(expr("sum(quantity * price)")).collect()[0][0]

result_df =  ( raw_cust_df.groupBy("productid")
                         .agg( sum(expr("quantity * price")).alias("product_revenue"),
                              (sum(expr("quantity * price")) * 100.0 / total_revenue ).alias("percent")
                             )
             )


result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------


# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame.
# Basically the "raw_cust_df" dataframe was built, using the FOOL format, from the customers.csv file 
raw_cust_df.createOrReplaceTempView('customers') # We give the view  a name 'customers'
#raw_ord_df.createOrReplaceTempView('orders')   # We give the view  a name 'orders'

# Run the SQL query using spark.sql()
sql_query="""
               SELECT productid 
                     ,sum(quantity * price) AS product_revenue
                     ,sum(quantity * price) * 100.0 / (SELECT sum(quantity * price) FROM dev.spark_db.customers) AS percent
               FROM   dev.spark_db.customers    AS C
               GROUP BY productid

;
"""

result_df =  spark.sql(sql_query)
result_df.display()



# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### Retrieve customers who made two consecutive purchases (2 days)
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC WITH CTE AS (
# MAGIC                SELECT customerid, order_id, order_date
# MAGIC                      ,LAG(order_date) OVER(PARTITION BY customerid ORDER BY order_date) AS previous_purchase
# MAGIC                FROM   dev.spark_db.customers    AS C
# MAGIC                GROUP BY customerid, order_id, order_date
# MAGIC             )
# MAGIC SELECT * 
# MAGIC FROM CTE
# MAGIC WHERE (PREVIOUS_PURCHASE + INTERVAL 1 DAY) = ORDER_DATE
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark
# MAGIC
# MAGIC Method 1: Using DataFrame API (Recommended)
# MAGIC
# MAGIC Method 2: Using a TemporaryView and Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)

# COMMAND ----------

"""from pyspark.sql.functions import expr, col, count, min, max, sum, avg, lag
from pyspark.sql.window import Window

windowSpec = Window.partitionBy("CUSTOMERID").orderBy("ORDER_DATE") # THIS PIECE IS REPLACING WHAT THE OVER() PART OF THE WINDOW FUNCTION DOES

result_df =  ( raw_cust_df.select("customerid", "price", "ORDER_DATE")
                          .withColumn("Prev_PUR_DATE", lag("ORDER_DATE").over(windowSpec))
             )

result_df.display()
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------


# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame.
# Basically the "raw_cust_df" dataframe was built, using the FOOL format, from the customers.csv file 
raw_cust_df.createOrReplaceTempView('customers') # We give the view  a name 'customers'
#raw_ord_df.createOrReplaceTempView('orders')   # We give the view  a name 'orders'

# Run the SQL query using spark.sql()
sql_query="""
WITH CTE AS (
               SELECT customerid, order_id, order_date
                     ,LAG(order_date) OVER(PARTITION BY customerid ORDER BY order_date) AS previous_purchase
               FROM   dev.spark_db.customers    AS C
               GROUP BY customerid, order_id, order_date
            )
SELECT * 
FROM CTE
WHERE (PREVIOUS_PURCHASE + INTERVAL 1 DAY) = ORDER_DATE
;
"""

result_df =  spark.sql(sql_query)
result_df.display()



# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### Find churned customers(no orders in last 6 months) 
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC                SELECT customerid, order_date, MAX(order_date) AS last_order_date
# MAGIC                FROM   dev.spark_db.customers    AS C
# MAGIC                GROUP BY customerid, order_date
# MAGIC                HAVING MAX(order_date) < (NOW() - INTERVAL 6 MONTH)

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark
# MAGIC
# MAGIC Method 1: Using DataFrame API (Recommended)
# MAGIC
# MAGIC Method 2: Using a TemporaryView and Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)

# COMMAND ----------

from pyspark.sql.functions import expr, col, count, min, max, sum, avg
#from pyspark.sql.window import Window


result_df =  ( raw_cust_df.groupBy("customerid", "order_date")
                           .agg( max("order_date").alias("last_order_date") )
                           .filter( col("last_order_date") < expr("current_date() - INTERVAL 6 MONTH") )
             )


result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------


# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame.
# Basically the "raw_cust_df" dataframe was built, using the FOOL format, from the customers.csv file 
raw_cust_df.createOrReplaceTempView('customers') # We give the view  a name 'customers'
#raw_ord_df.createOrReplaceTempView('orders')   # We give the view  a name 'orders'

# Run the SQL query using spark.sql()
sql_query="""
               SELECT customerid, order_date, MAX(order_date) AS last_order_date
               FROM   dev.spark_db.customers    AS C
               GROUP BY customerid, order_date
               HAVING MAX(order_date) < (NOW() - INTERVAL 6 MONTH)
;
"""

result_df =  spark.sql(sql_query)
result_df.display()



# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### Calculate cumulative reveneue by day
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC                SELECT order_date,SUM(quantity * price) AS revenue
# MAGIC                      ,SUM(SUM(quantity * price) ) OVER(ORDER BY order_date) AS daily_revenue
# MAGIC                FROM   dev.spark_db.customers    AS C
# MAGIC                GROUP BY order_date

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark
# MAGIC
# MAGIC Method 1: Using DataFrame API (Recommended)
# MAGIC
# MAGIC Method 2: Using a TemporaryView and Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)

# COMMAND ----------

from pyspark.sql.functions import expr, col, count, min, max, sum, avg
#from pyspark.sql.window import Window

result_df =  ( raw_cust_df.groupBy("order_date")
                          .agg( sum( col("quantity") * col("price")).alias("revenue") )
                          .withColumn("daily_revenue", expr("SUM(revenue) OVER(ORDER BY order_date)") )
             )

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------


# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame.
# Basically the "raw_cust_df" dataframe was built, using the FOOL format, from the customers.csv file 
raw_cust_df.createOrReplaceTempView('customers') # We give the view  a name 'customers'
#raw_ord_df.createOrReplaceTempView('orders')   # We give the view  a name 'orders'

# Run the SQL query using spark.sql()
sql_query="""
               SELECT order_date,SUM(quantity * price) AS revenue
                     ,SUM(SUM(quantity * price) ) OVER(ORDER BY order_date) AS daily_revenue
               FROM   dev.spark_db.customers    AS C
               GROUP BY order_date
;
"""

result_df =  spark.sql(sql_query)
result_df.display()



# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### Find customers who ordered more than the average number of orders per customer
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH CTE AS ( SELECT  customerid, count(order_id) as ORDER_CNT
# MAGIC               FROM   dev.spark_db.customers  AS D
# MAGIC               GROUP BY CUSTOMERID
# MAGIC             )
# MAGIC SELECT DISTINCT 
# MAGIC        customerid, ORDER_CNT
# MAGIC       ,(SELECT AVG(ORDER_CNT) FROM CTE ) AS GRL_AVG_ORDER_CNT
# MAGIC FROM CTE
# MAGIC GROUP BY customerid, ORDER_CNT
# MAGIC HAVING ORDER_CNT > (SELECT AVG(ORDER_CNT) FROM CTE)
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark
# MAGIC
# MAGIC Method 1: Using DataFrame API (Recommended)
# MAGIC
# MAGIC Method 2: Using a TemporaryView and Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------


# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame.
# Basically the "raw_cust_df" dataframe was built, using the FOOL format, from the customers.csv file 
raw_cust_df.createOrReplaceTempView('customers') # We give the view  a name 'customers'
#raw_ord_df.createOrReplaceTempView('orders')   # We give the view  a name 'orders'

# Run the SQL query using spark.sql()
sql_query="""
WITH CTE AS ( SELECT  customerid, count(order_id) as ORDER_CNT
              FROM   dev.spark_db.customers  AS D
              GROUP BY CUSTOMERID
            )
SELECT DISTINCT 
       customerid, ORDER_CNT
      ,(SELECT AVG(ORDER_CNT) FROM CTE ) AS GRL_AVG_ORDER_CNT
FROM CTE
GROUP BY customerid, ORDER_CNT
HAVING ORDER_CNT > (SELECT AVG(ORDER_CNT) FROM CTE)
;
"""

result_df =  spark.sql(sql_query)
result_df.display()



# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### Find products that contribute to 80% of the revenue(pareto analysis)
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

Main structure to tackle this challenge
"""
1 I need to BREAK the code into 4 parts
P1: CTE1 to get revenue by product 
P2: CTE2 to get total revenue; base on the CTE1 data
P3: Select product, product_revenue and cumulative which come from a subquery
     The subquery uses 
     -A cross join of the CTE1 with CTE2 and 
     -A window function to calculate the cumulative revenue out of the product_revenue
P4: Filter the result to get products that contribute to +80% of the revenue; cumulative >= 80%

     CTE1 AS ( SELECT PRODUCTID, SUM(QUANTITY * PRICE) AS P_REVENUE
               FROM dev.spark_db.customers as C
               GROUP BY PRODUCTID
             ),

     CTE2 AS ( SELECT SUM(P_REVENUE) AS TOTAL
               FROM CTE1
             )

SELECT        PRODUCTID, P_REVENUE, CUMULATIVE
FROM ( SELECT PRODUCTID, P_REVENUE, TOTAL
              ,SUM(P_REVENUE) OVER( ORDER BY P_REVENUE DESC) AS CUMULATIVE
       FROM CTE1
       CROSS JOIN CTE2
       GROUP BY PRODUCTID, P_REVENUE, TOTAL
     ) AS T
WHERE CUMULATIVE <= TOTAL * 0.80

;
"""


# COMMAND ----------

# MAGIC %sql
# MAGIC WITH CTE1 AS ( SELECT PRODUCTID, SUM(QUANTITY * PRICE) AS P_REVENUE
# MAGIC                FROM dev.spark_db.customers as C
# MAGIC                GROUP BY PRODUCTID
# MAGIC              ),
# MAGIC
# MAGIC      CTE2 AS ( SELECT SUM(P_REVENUE) AS TOTAL
# MAGIC                FROM CTE1
# MAGIC              )
# MAGIC
# MAGIC SELECT        PRODUCTID, P_REVENUE, CUMULATIVE
# MAGIC FROM ( SELECT PRODUCTID, P_REVENUE, TOTAL
# MAGIC               ,SUM(P_REVENUE) OVER( ORDER BY P_REVENUE DESC) AS CUMULATIVE
# MAGIC        FROM CTE1
# MAGIC        CROSS JOIN CTE2
# MAGIC      ) AS T
# MAGIC WHERE CUMULATIVE <= TOTAL * 0.85
# MAGIC ORDER BY P_REVENUE DESC
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark
# MAGIC
# MAGIC Method 1: Using DataFrame API (Recommended)
# MAGIC
# MAGIC Method 2: Using a TemporaryView and Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------


# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame.
# Basically the "raw_cust_df" dataframe was built, using the FOOL format, from the customers.csv file 
raw_cust_df.createOrReplaceTempView('customers') # We give the view  a name 'customers'
#raw_ord_df.createOrReplaceTempView('orders')   # We give the view  a name 'orders'

# Run the SQL query using spark.sql()
sql_query="""
WITH CTE1 AS ( SELECT PRODUCTID, SUM(QUANTITY * PRICE) AS P_REVENUE
               FROM dev.spark_db.customers as C
               GROUP BY PRODUCTID
             ),

     CTE2 AS ( SELECT SUM(P_REVENUE) AS TOTAL
               FROM CTE1
             )

SELECT        PRODUCTID, P_REVENUE, CUMULATIVE
FROM ( SELECT PRODUCTID, P_REVENUE, TOTAL
              ,SUM(P_REVENUE) OVER( ORDER BY P_REVENUE DESC) AS CUMULATIVE
       FROM CTE1
       CROSS JOIN CTE2
     ) AS T
WHERE CUMULATIVE <= TOTAL * 0.85
ORDER BY P_REVENUE DESC
;
"""

result_df =  spark.sql(sql_query)
result_df.display()



# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### Show last purchase for each customer along with order amount
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC                 SELECT customerid, MAX(order_id) as LAST_ORDER, SUM(QUANTITY * PRICE) AS P_REVENUE
# MAGIC                 FROM dev.spark_db.customers as C
# MAGIC                 GROUP BY customerid

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark
# MAGIC
# MAGIC Method 1: Using DataFrame API (Recommended)
# MAGIC
# MAGIC Method 2: Using a TemporaryView and Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)

# COMMAND ----------

result_df =  ( raw_cust_df.groupBy("customerid")
                          .agg( max("order_id").alias("LAST_ORDER"), 
                                sum(expr("quantity * price")).alias("ORDER_AMOUNT") 
                              )
             )


result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------


# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame.
# Basically the "raw_cust_df" dataframe was built, using the FOOL format, from the customers.csv file 
raw_cust_df.createOrReplaceTempView('customers') # We give the view  a name 'customers'
#raw_ord_df.createOrReplaceTempView('orders')   # We give the view  a name 'orders'

# Run the SQL query using spark.sql()
sql_query="""
                SELECT customerid, MAX(order_id) as LAST_ORDER, SUM(QUANTITY * PRICE) AS P_REVENUE
                FROM customers as C
                GROUP BY customerid
;
"""

result_df =  spark.sql(sql_query)
result_df.display()



# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### Calculate Average time between two purhcases for each customer
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- LAG Partition by CUSTOMERID so that we get the previous purchase date for the same customer.
# MAGIC -- COALESCE  fills nulls records with 0 so they are considered in the average count cause, basically LAG gets null when there is no previous purchase 
# MAGIC -- and nulls are not counted in the average  and so we need to fill them with 0 to get the average of all the purchases.
# MAGIC
# MAGIC WITH PREV_PUR AS (
# MAGIC                   SELECT CUSTOMERID, ORDER_DATE
# MAGIC                          ,LAG(ORDER_DATE) OVER(PARTITION BY CUSTOMERID ORDER BY ORDER_DATE) AS PREVIOUS_PUR_DATE
# MAGIC                          ,COALESCE( DATEDIFF(ORDER_DATE, LAG(ORDER_DATE) OVER(PARTITION BY CUSTOMERID ORDER BY ORDER_DATE) ), 0) AS DIFF
# MAGIC                   FROM dev.spark_db.customers
# MAGIC                   order by customerid
# MAGIC                  ),
# MAGIC     SUM_DFF AS (
# MAGIC                   SELECT CUSTOMERID, ORDER_DATE, PREVIOUS_PUR_DATE, SUM(DIFF) AS SUM_DIFF
# MAGIC                   FROM PREV_PUR
# MAGIC                   GROUP BY CUSTOMERID, ORDER_DATE, PREVIOUS_PUR_DATE
# MAGIC                  )
# MAGIC
# MAGIC SELECT CUSTOMERID, AVG(SUM_DIFF) AS AVG_DIFF
# MAGIC FROM SUM_DFF 
# MAGIC GROUP BY CUSTOMERID
# MAGIC ORDER BY CUSTOMERID DESC
# MAGIC ;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark
# MAGIC
# MAGIC Method 1: Using DataFrame API (Recommended)
# MAGIC
# MAGIC Method 2: Using a TemporaryView and Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------


# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame.
# Basically the "raw_cust_df" dataframe was built, using the FOOL format, from the customers.csv file 
raw_cust_df.createOrReplaceTempView('customers') # We give the view  a name 'customers'
#raw_ord_df.createOrReplaceTempView('orders')   # We give the view  a name 'orders'

# Run the SQL query using spark.sql()
sql_query="""
WITH PREV_PUR AS (
                  SELECT CUSTOMERID, ORDER_DATE
                         ,LAG(ORDER_DATE) OVER(PARTITION BY CUSTOMERID ORDER BY ORDER_DATE) AS PREVIOUS_PUR_DATE
                         ,COALESCE( DATEDIFF(ORDER_DATE, LAG(ORDER_DATE) OVER(PARTITION BY CUSTOMERID ORDER BY ORDER_DATE) ), 0) AS DIFF
                  FROM customers
                  order by customerid
                 ),
    SUM_DFF AS (
                  SELECT CUSTOMERID, ORDER_DATE, PREVIOUS_PUR_DATE, SUM(DIFF) AS SUM_DIFF
                  FROM PREV_PUR
                  GROUP BY CUSTOMERID, ORDER_DATE, PREVIOUS_PUR_DATE
                 )

SELECT CUSTOMERID, AVG(SUM_DIFF) AS AVG_DIFF
FROM SUM_DFF 
GROUP BY CUSTOMERID
ORDER BY CUSTOMERID DESC
;
"""

result_df =  spark.sql(sql_query)
result_df.display()



# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### Calculate YoY growth in revenue
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --YoY formu;a:   formula ((Current Period Revenue - Prior Period Revenue) / Prior Period Revenue) * 100
# MAGIC WITH CTE1 AS (SELECT extract(year from ORDER_DATE) AS YR
# MAGIC                     ,SUM(QUANTITY * PRICE) AS REVENUE
# MAGIC                     ,LAG(SUM(QUANTITY * PRICE)) OVER(ORDER BY extract(year from ORDER_DATE)) AS PREV_REVENUE  
# MAGIC               FROM dev.spark_db.customers
# MAGIC               GROUP BY extract(year from ORDER_DATE ) 
# MAGIC               )
# MAGIC SELECT  YR, REVENUE, PREV_REVENUE
# MAGIC        ,(REVENUE- PREV_REVENUE) / PREV_REVENUE * 100 AS YOY_GROWTH
# MAGIC FROM CTE1
# MAGIC ORDER BY YR
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark
# MAGIC
# MAGIC Method 1: Using DataFrame API (Recommended)
# MAGIC
# MAGIC Method 2: Using a TemporaryView and Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------


# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame.
# Basically the "raw_cust_df" dataframe was built, using the FOOL format, from the customers.csv file 
raw_cust_df.createOrReplaceTempView('customers') # We give the view  a name 'customers'
#raw_ord_df.createOrReplaceTempView('orders')   # We give the view  a name 'orders'

# Run the SQL query using spark.sql()
sql_query="""
--YoY formu;a:   formula ((Current Period Revenue - Prior Period Revenue) / Prior Period Revenue) * 100
WITH CTE1 AS (SELECT extract(year from ORDER_DATE) AS YR
                    ,SUM(QUANTITY * PRICE) AS REVENUE
                    ,LAG(SUM(QUANTITY * PRICE)) OVER(ORDER BY extract(year from ORDER_DATE)) AS PREV_REVENUE  
              FROM dev.spark_db.customers
              GROUP BY extract(year from ORDER_DATE ) 
              )
SELECT  YR, REVENUE, PREV_REVENUE
       ,(REVENUE- PREV_REVENUE) / PREV_REVENUE * 100 AS YOY_GROWTH
FROM CTE1
ORDER BY YR;
"""

result_df =  spark.sql(sql_query)
result_df.display()



# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### Get the longest Gap between orders for each customer
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH CTE1 AS ( SELECT CUSTOMERID, ORDER_DATE
# MAGIC                      ,LAG(ORDER_DATE) OVER(ORDER BY ORDER_DATE) AS PREV_PUR_DATE
# MAGIC              
# MAGIC                FROM dev.spark_db.customers
# MAGIC              )
# MAGIC SELECT CUSTOMERID,  MAX(DATEDIFF(ORDER_DATE, PREV_PUR_DATE)) AS DIFF
# MAGIC FROM CTE1
# MAGIC GROUP BY CUSTOMERID
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark
# MAGIC
# MAGIC Method 1: Using DataFrame API (Recommended)
# MAGIC
# MAGIC Method 2: Using a TemporaryView and Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------


# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame.
# Basically the "raw_cust_df" dataframe was built, using the FOOL format, from the customers.csv file 
raw_cust_df.createOrReplaceTempView('customers') # We give the view  a name 'customers'
#raw_ord_df.createOrReplaceTempView('orders')   # We give the view  a name 'orders'

# Run the SQL query using spark.sql()
sql_query="""
WITH CTE1 AS ( SELECT CUSTOMERID, ORDER_DATE
                     ,LAG(ORDER_DATE) OVER(ORDER BY ORDER_DATE) AS PREV_PUR_DATE
             
               FROM dev.spark_db.customers
             )
SELECT CUSTOMERID,  MAX(DATEDIFF(ORDER_DATE, PREV_PUR_DATE)) AS DIFF
FROM CTE1
GROUP BY CUSTOMERID;
"""

result_df =  spark.sql(sql_query)
result_df.display()



# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### TBD
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark
# MAGIC
# MAGIC Method 1: Using DataFrame API (Recommended)
# MAGIC
# MAGIC Method 2: Using a TemporaryView and Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Practica #1: Number of distint products types
# MAGIC ----------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ##### SQL query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT PRODUCTID) AS DISTINCT_PRODUCT_COUNT 
# MAGIC FROM dev.spark_db.customers
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### With Pyspark transformation
# MAGIC
# MAGIC ##### Take the same piece of the SQL query above : 
# MAGIC
# MAGIC COUNT(DISTINCT PRODUCTID) AS DISTINCT_PRODUCT_COUNT 
# MAGIC
# MAGIC ##### and, in pyspark, put into the:
# MAGIC
# MAGIC  selectExpr('COUNT(DISTINCT PRODUCTID) AS DISTINCT_PRODUCT_COUNT ')

# COMMAND ----------

res_cust_df = (cust_df.selectExpr('count(distinct productid) as distinct_product_count') 
              )
res_cust_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Practice#2: Customers with consecutive purchases (2 days)
# MAGIC ------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ##### SQL query

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH Prev_pur as ( SELECT CUSTOMERID, ORDER_ID, ORDER_DATE, LAG(ORDER_DATE) OVER( ORDER BY ORDER_DATE ) AS Prev_purchase 
# MAGIC                    FROM dev.spark_db.customers
# MAGIC                    ORDER BY CUSTOMERID
# MAGIC                  )
# MAGIC              SELECT *
# MAGIC              FROM Prev_pur
# MAGIC              WHERE (PREV_PURCHASE + INTERVAL 1 DAY) = ORDER_DATE
# MAGIC              --DATEDIFF(ORDER_DATE , Prev_purchase ) =1 
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark

# COMMAND ----------

query = """
        WITH Prev_pur as ( SELECT CUSTOMERID, ORDER_ID, ORDER_DATE, LAG(ORDER_DATE) OVER( ORDER BY ORDER_DATE ) AS Prev_purchase 
                   FROM dev.spark_db.customers
                   ORDER BY CUSTOMERID
                 )
             SELECT *
             FROM Prev_pur
             WHERE (PREV_PURCHASE + INTERVAL 1 DAY) = ORDER_DATE
             --DATEDIFF(ORDER_DATE , Prev_purchase ) =1 
        """
result= spark.sql(query)
display(result)

# COMMAND ----------


data_schema = "id int, source string , destination string, distance int"

data_list= [(101, "Mumbai", "Goa", 587),
            (102, "Mumbai", "Bangalore", 985),
            (102, "Mumbai", "Bangalore", 985),
            (103, "Dheli", "Chennai", 2208),
            (104, "Dheli", "Chennai", 2208),     
            (105, "Bangalore", "Kolkata", 1868),             
            (105, "Bangalore", "Kolkata", 1865)                           
            ]
#df = spark.createDataFrame(data=data_list, schema=data_schema)
df=pd.DataFrame(data_list)

df.display()

# COMMAND ----------

query = """
SELECT id, source
from df 
"""

result = duckdb.query(query).df()
print(result)

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### 1 TBD
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1 TBD
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1 TBD
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark

# COMMAND ----------

raw_cust_df = ( spark.read.format('csv')
                          .option("header", True)
                          .option("inferSchema", True)
                          .load("/Volumes/dev/spark_db/datasets/spark_programmingHBL/data/customers.csv")
              )

#raw_cust_df.display()


# COMMAND ----------

from pyspark.sql.functions import expr, col, count, min, max, sum, avg, lag
#from pyspark.sql.window import Window

result_df = ( raw_cust_df.drop( "productid") # to drop multiple columnns just add a ',' a comma between the column names    
            ) 

result_df.display()

# COMMAND ----------

