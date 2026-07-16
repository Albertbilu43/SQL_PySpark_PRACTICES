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
# MAGIC        # Fix the order_date as it was incorrectly inferred
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
# MAGIC ###### SQL Query way --> Write a normal SQL query 
# MAGIC
# MAGIC ###### PySpark transformations way --> IDEAL to perform the steps of an SQL query
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
from pyspark.sql.functions import to_date, col

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
                          .load("/Volumes/dev/spark_db/datasets/spark_programming/data/customers.csv")
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
# MAGIC 1 withColumn() to add a column or replacing the existing column that has the same name. 
# MAGIC
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumn.html
# MAGIC
# MAGIC 2 withColumns() to add multiple columns or replacing the existing columns that have the same names.
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
# MAGIC -----------------------------------------------------------

# COMMAND ----------

spark.version

# COMMAND ----------

pip install duckdb pandas

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
import duckdb
from pyspark.sql.functions import col, expr
from pyspark.sql.types     import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

raw_cust_df = ( spark.read.format('csv')
                          .option("header", True)
                          .option("inferSchema", True)
                          .load("/Volumes/dev/spark_db/datasets/spark_programmingHBL/data/customers.csv")
              )

#raw_cust_df.display()

raw_ord_df = ( spark.read.format('csv')
                          .option("header", True)
                          .option("inferSchema", True)
                          .load("/Volumes/dev/spark_db/datasets/spark_programmingHBL/data/orders.csv")
              )


raw_prod_df = ( spark.read.format('csv')
                          .option("header", True)
                          .option("inferSchema", True)
                          .load("/Volumes/dev/spark_db/datasets/spark_programmingHBL/data/products.csv")
              )


raw_ret_df = ( spark.read.format('csv')
                          .option("header", True)
                          .option("inferSchema", True)
                          .load("/Volumes/dev/spark_db/datasets/spark_programmingHBL/data/returns.csv")
              )


raw_emp_df = ( spark.read.format('csv')
                          .option("header", True)
                          .option("inferSchema", True)
                          .load("/Volumes/dev/spark_db/datasets/spark_programming/data/employee.csv")
              )

raw_dep_df = ( spark.read.format('csv')
                         .option("header", True)
                         .option("inferSchema", True)
                         .load("/Volumes/dev/spark_db/datasets/spark_programming/data/department.csv")
              )



# COMMAND ----------

27# Option One reading csv file from Volume

#raw_cust_df.display()
#raw_ord_df.display()
#raw_prod_df.display()
#raw_ret_df.display()

#raw_emp_df.display()

#raw_dep_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032783996.png](./image_1774032783996.png "image_1774032783996.png")
# MAGIC
# MAGIC #### 10-50 SQL Interview Questions 
# MAGIC
# MAGIC https://www.linkedin.com/feed/update/urn:li:activity:7402591561454383104/?updateEntityUrn=urn%3Ali%3Afs_updateV2%3A%28urn%3Ali%3Aactivity%3A7402591561454383104%2CFEED_DETAIL%2CEMPTY%2CDEFAULT%2Cfalse%29
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
# MAGIC ###### 2 Apply transformations(Composable query: All steps in encapsualted) 
# MAGIC
# MAGIC ##### Now you have to use the newly created dataframe "raw_cust_df"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #####Remember: If using select() and selectExpr() 
# MAGIC   - select() uses the DataFrame API's column objects and functions
# MAGIC   - selectExpr() accepts SQL-style expressions as strings.
# MAGIC     -     But be careful when using groupBy() and agg(). See the notebook "Instructions..."
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###STANDARD WAY
# MAGIC ##### The piece of code below is the STANDARD WAY of performing groupBy and aggregations 

# COMMAND ----------

from pyspark.sql.functions import to_date, col, expr, sum, count


result_df =(  raw_cust_df.groupBy("productid")
                         .agg(expr("sum(quantity * price)  as Revenue"))
                         .orderBy("Revenue", ascending=False)
                         .limit(3)
           )
            
result_df.display()



# COMMAND ----------

"""
from pyspark.sql.functions import expr

result_df = ( cust_df.select("customerid", "customer_name", "productid", "quantity", "price")
                     .where("productid IS NOT NULL")
                     .groupBy("productid").agg(expr("sum(quantity * price)").alias("sales"))
                     .orderBy("sales", ascending=False)
                     .limit(3)
            )

result_df.display()            
"""

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

# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 1: Using DataFrame API (Recommended)
# MAGIC This approach is the most common and "PySpark way" to achieve the result.

# COMMAND ----------

from pyspark.sql.functions import col, count, expr

grouped_df = ( raw_cust_df.groupBy("customerid")
                          .agg(expr("count(*) as countids") )
                          .filter("countids > 1") # This works as the 'having' filter for group by in SQL
             )

grouped_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Other ways

# COMMAND ----------

from pyspark.sql.functions import col, count


# 1. Group by 'customerid' and count the occurrences, giving the count column an alias
#    (e.g., 'customer_count')
grouped_df = (raw_cust_df.groupBy("customerid") \
                        .agg(count("*").alias("customer_count")) #
             )
# 2. Filter the result to keep only rows where the count is greater than 1
result_df = grouped_df.filter(col("customer_count") > 1) #

# 3. Show the result
result_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 2: Using Spark SQL Temporary View
# MAGIC
# MAGIC You can also use raw SQL directly within PySpark by registering your DataFrame as a temporary view.

# COMMAND ----------

from pyspark.sql.functions import expr, col, count

# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame
raw_cust_df.createOrReplaceTempView("customers")

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
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 02 Retrieve the second highest salary from employee table
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT MAX(salary) as SECOND_HIGHEST_SALARY
# MAGIC FROM   dev.spark_db.employee as e
# MAGIC WHERE  SALARY < (SELECT MAX(SALARY) FROM dev.spark_db.employee)
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 1: Using DataFrame API (Recommended)
# MAGIC This approach is the most common and "PySpark way" to achieve the result.

# COMMAND ----------

from pyspark.sql.functions import col, count, expr, max

MAX_SALARY = raw_emp_df.agg(max("salary")).first()[0] # max_salary

result_df = ( raw_emp_df.where(col('salary') < MAX_SALARY)
                        .agg(max("salary")).alias("SECOND_TOP_SALARY")
            )

result_df.display()

# COMMAND ----------

"""

from pyspark.sql.functions import to_date, col, expr, sum, count, max 

max_salary = raw_emp_df.agg(max("salary")).first()[0] # max_salary

# Then filter for salaries less than max and get the second highest
result_df = (raw_emp_df.filter(col("salary") < max_salary)
                       .agg(max("salary").alias("SECOND_TOP_SALARY"))
            )

result_df.display()

# First, get the maximum salary
"""

"""
Esta línea de código se utiliza comúnmente en
PySpark (o Spark con Python) para extraer un único valor numérico —en este caso, el salario más alto— de un DataFrame y almacenarlo en una variable de Python.
Desglose del código:

    raw_emp_df: Es el nombre del DataFrame que contiene los datos de los empleados.
    .agg(max("salary")):
        agg es la función de agregación.
        max_("salary") calcula el valor máximo de la columna llamada "salary".
        Nota: El resultado de este paso sigue siendo un DataFrame con una sola fila y una sola columna.
    .first():
        Esta es una "acción" de Spark que toma la primera fila del DataFrame resultante y la devuelve como un objeto tipo Row de Spark
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Other ways

# COMMAND ----------

from pyspark.sql.functions import col, count


# 1. 

# 3. Show the result
result_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 2: Using Spark SQL Temporary View
# MAGIC
# MAGIC You can also use raw SQL directly within PySpark by registering your DataFrame as a temporary view.

# COMMAND ----------

from pyspark.sql.functions import expr, col, count

# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame
raw_emp_df.createOrReplaceTempView("employee")

# Run the SQL query using spark.sql()
sql_query = """
SELECT MAX(salary) as SECOND_HIGHEST_SALARY
FROM   dev.spark_db.employee as e
WHERE  SALARY < (SELECT MAX(SALARY) FROM dev.spark_db.employee)
;
"""
result_df = spark.sql(sql_query)

# Show the result
result_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ####3: Find employees without department
# MAGIC ----------------------------------------------------
# MAGIC
# MAGIC ##### SQL query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT name, departmentid
# MAGIC FROM dev.spark_db.employee
# MAGIC WHERE departmentid is NULL
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 1: Using DataFrame API (Recommended)
# MAGIC This approach is the most common and "PySpark way" to achieve the result.

# COMMAND ----------

from pyspark.sql.functions import col, expr, max, count, min
from pyspark.sql.types     import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------


emp_df = ( raw_emp_df.filter(raw_emp_df.departmentid.isNull())
                      .select('name', 'departmentid')
         )

emp_df.display()         

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Other ways

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 2: Using Spark SQL Temporary View
# MAGIC
# MAGIC You can also use raw SQL directly within PySpark by registering your DataFrame as a temporary view.

# COMMAND ----------

from pyspark.sql.functions import expr, col, count

# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame
raw_emp_df.createOrReplaceTempView("employee")

# Run the SQL query using spark.sql()
sql_query = """
SELECT name, departmentid
FROM dev.spark_db.employee
WHERE departmentid is NULL
;
"""
result_df = spark.sql(sql_query)

# Show the result
result_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####4: Total revenue per product
# MAGIC ----------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ##### SQL query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT PRODUCT_NAME, SUM(QUANTITY * PRICE) AS SALES
# MAGIC FROM dev.spark_db.customers
# MAGIC --FROM dev.spark_db.employee
# MAGIC GROUP BY PRODUCT_NAME;

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 1: Using DataFrame API (Recommended)
# MAGIC This approach is the most common and "PySpark way" to achieve the result.

# COMMAND ----------

from pyspark.sql.functions import col, expr, max, count, min, sum, count
from pyspark.sql.types     import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------


result_df = ( raw_cust_df.groupBy('product_name')
                         .agg(expr('sum(quantity * price)').alias('sales'))
            )
result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Other ways

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 2: Using Spark SQL Temporary View
# MAGIC
# MAGIC You can also use raw SQL directly within PySpark by registering your DataFrame as a temporary view.

# COMMAND ----------

from pyspark.sql.functions import expr, col, count

# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame
raw_emp_df.createOrReplaceTempView("employee")

# Run the SQL query using spark.sql()
sql_query = """

;
"""
result_df = spark.sql(sql_query)

# Show the result
result_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####5: Top three highest paid employees
# MAGIC ----------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ##### SQL query

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH T1 AS (SELECT NAME, SALARY
# MAGIC                    ,DENSE_RANK() OVER( ORDER BY SALARY DESC) AS RNK
# MAGIC             FROM   dev.spark_db.employee
# MAGIC            )
# MAGIC SELECT *
# MAGIC FROM T1
# MAGIC WHERE RNK IN ( 1, 2, 3)
# MAGIC LIMIT 3;            

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 1: Using DataFrame API (Recommended)
# MAGIC This approach is the most common and "PySpark way" to achieve the result.

# COMMAND ----------

from pyspark.sql.functions import col, expr, max, count, min
from pyspark.sql.types     import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ###### Other ways

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 2: Using Spark SQL Temporary View
# MAGIC
# MAGIC You can also use raw SQL directly within PySpark by registering your DataFrame as a temporary view.

# COMMAND ----------

from pyspark.sql.functions import expr, col, count

# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame
raw_emp_df.createOrReplaceTempView("employee")

# Run the SQL query using spark.sql()
sql_query = """
WITH T1 AS (SELECT NAME, SALARY
                   ,DENSE_RANK() OVER( ORDER BY SALARY DESC) AS RNK
            FROM   employee
           )
SELECT *
FROM T1
WHERE RNK IN ( 1, 2, 3)
LIMIT 3;    
;
"""
result_df = spark.sql(sql_query)

# Show the result
result_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####6: CUSTOMERS WHO MADE PURCHASES BUY NEVER RETURNED PRODUCTS
# MAGIC ----------------------------------------------------
# MAGIC
# MAGIC ##### SQL query

# COMMAND ----------

#raw_cust_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT    DISTINCT CUSTOMER_NAME
# MAGIC FROM      dev.spark_db.customers as c
# MAGIC LEFT JOIN dev.spark_db.returns   as r  on c.customerid = r.customerid
# MAGIC WHERE     r.customerid is NULL;
# MAGIC --FROM dev.spark_db.employee

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 1: Using DataFrame API (Recommended)
# MAGIC This approach is the most common and "PySpark way" to achieve the result.

# COMMAND ----------

from pyspark.sql.functions import col, expr, max, count, min, sum
from pyspark.sql.types     import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

#

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Other ways

# COMMAND ----------



""" CREATE TABLES and CREATE A JOINING EXPRESSION(using columns)
OPTION ONE

# Here we are NOT using the spark.read. FOOL format, instead we call the tables, which now contain data, directly spark.table...
members_df = spark.table("dev.spark_db.members").alias("m") 
bookings_df = spark.table("dev.spark_db.bookings").alias("b")

#join_expr = expr("m.member_id == b.member_id")
join_expr = col("m.member_id") == col("b.member_id") # Here we save the join condition/expression in a variable

reports_df = (
    members_df.join(bookings_df, join_expr, "inner") # Here we use teh join condition/expression variable
        .filter("m.last_name == 'Smith' and b.slots > 5")
        .select("m.member_id", "m.first_name", "m.last_name", "b.facility_id", "b.slots", "b.start_time")
        .orderBy("m.first_name", col("b.slots").desc())
)
"""

#------------------------------------------------------------------------------------------------------


""" OPTION TWO
bookings_df = spark.table("dev.spark_db.bookings").alias("b")
members_df = spark.table("dev.spark_db.members").alias("m")
facilities_df = spark.table("dev.spark_db.facilities").alias("f")

report_df = (bookings_df.join(members_df, expr("b.member_id == m.member_id"), "inner")
                        .join(facilities_df, col("b.facility_id") == col("f.facility_id"), "inner")
                        .filter("m.last_name == 'Smith' and b.slots > 5")
                        .selectExpr("m.member_id", "m.first_name", "m.last_name", "f.facility_name", "b.slots", "b.slots * f.member_cost as booking_amount", "b.start_time")
                        .orderBy(col("m.first_name").asc(),col("booking_amount").desc())
)
"""

# COMMAND ----------


cust_df= spark.table("dev.spark_db.customers").alias('c')
ret_df = spark.table("dev.spark_db.returns").alias('r')

join_expr = expr("c.customerid == r.customerid")
#join_expr  = col("c.customerid") == col("r.customerid")


result_df = ( cust_df.join(ret_df, join_expr, 'left')
               .filter(ret_df.customerid.isNull())
               .select("customer_name").distinct()

         )

result_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 2: Using Spark SQL Temporary View
# MAGIC
# MAGIC You can also use raw SQL directly within PySpark by registering your DataFrame as a temporary view.

# COMMAND ----------

from pyspark.sql.functions import expr, col, count

# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame
raw_emp_df.createOrReplaceTempView("employee")

# Run the SQL query using spark.sql()
sql_query = """

;
"""
result_df = spark.sql(sql_query)

# Show the result
result_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####7: cCount of orders per customer
# MAGIC ----------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ##### SQL query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CUSTOMER_NAME, COUNT(order_id) as OR_CNT
# MAGIC FROM dev.spark_db.customers
# MAGIC --FROM dev.spark_db.employee
# MAGIC GROUP BY CUSTOMER_NAME

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 1: Using DataFrame API (Recommended)
# MAGIC This approach is the most common and "PySpark way" to achieve the result.

# COMMAND ----------

from pyspark.sql.functions import col, expr, max, count, min
from pyspark.sql.types     import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------


cust_df = ( raw_cust_df.groupBy("CUSTOMER_NAME")
                       .agg(count("order_id").alias("OR_CNT"))

              ) 
cust_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###### Other ways

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 2: Using Spark SQL Temporary View
# MAGIC
# MAGIC You can also use raw SQL directly within PySpark by registering your DataFrame as a temporary view.

# COMMAND ----------

from pyspark.sql.functions import expr, col, count

# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame
raw_emp_df.createOrReplaceTempView("employee")

# Run the SQL query using spark.sql()
sql_query = """

;
"""
result_df = spark.sql(sql_query)

# Show the result
result_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####8: Retrieve all employees who joined in 2023
# MAGIC ----------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ##### SQL query

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT NAME, EXTRACT(YEAR FROM STARTDATE)AS YR
# MAGIC --FROM dev.spark_db.customers
# MAGIC FROM dev.spark_db.employee
# MAGIC WHERE extract(YEAR FROM STARTDATE) = 2023;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 1: Using DataFrame API (Recommended)
# MAGIC This approach is the most common and "PySpark way" to achieve the result.

# COMMAND ----------

from pyspark.sql.functions import col, expr, max, count, min, year
from pyspark.sql.types     import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------


emp_df = ( raw_emp_df.filter( year(col('STARTDATE')) == 2023)
                     .select(col('Name'))
         ) 
emp_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Other ways

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 2: Using Spark SQL Temporary View
# MAGIC
# MAGIC You can also use raw SQL directly within PySpark by registering your DataFrame as a temporary view.

# COMMAND ----------

from pyspark.sql.functions import expr, col, count

# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame
raw_emp_df.createOrReplaceTempView("employee")

# Run the SQL query using spark.sql()
sql_query = """

;
"""
result_df = spark.sql(sql_query)

# Show the result
result_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####9: Calculate average order value per customer
# MAGIC ----------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ##### SQL query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CUSTOMER_NAME, AVG(QUANTITY * PRICE) AS AVG_VALUE
# MAGIC FROM dev.spark_db.customers
# MAGIC --FROM dev.spark_db.employee
# MAGIC GROUP BY CUSTOMER_NAME;

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 1: Using DataFrame API (Recommended)
# MAGIC This approach is the most common and "PySpark way" to achieve the result.

# COMMAND ----------

from pyspark.sql.functions import col, expr, max, count, min, avg
from pyspark.sql.types     import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

cust_df = ( raw_cust_df.groupBy("customer_name")
                       .agg( avg( col("price") * col("quantity") ).alias("AVG_VALUE") )
          ) 
cust_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Other ways

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 2: Using Spark SQL Temporary View
# MAGIC
# MAGIC You can also use raw SQL directly within PySpark by registering your DataFrame as a temporary view.

# COMMAND ----------

from pyspark.sql.functions import expr, col, count

# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame
raw_emp_df.createOrReplaceTempView("employee")

# Run the SQL query using spark.sql()
sql_query = """

;
"""
result_df = spark.sql(sql_query)

# Show the result
result_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####10: Latest order placed by each customer
# MAGIC ----------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ##### SQL query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CUSTOMER_NAME ,MAX(ORDER_DATE) AS LAST_ORDER_DATE
# MAGIC FROM dev.spark_db.customers
# MAGIC --FROM dev.spark_db.employee
# MAGIC GROUP BY CUSTOMER_NAME
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 1: Using DataFrame API (Recommended)
# MAGIC This approach is the most common and "PySpark way" to achieve the result.

# COMMAND ----------

from pyspark.sql.functions import col, expr, max, count, min
from pyspark.sql.types     import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------


cust_df = ( raw_cust_df.groupBy('CUSTOMER_NAME')
                       .agg(max('ORDER_DATE').alias('LAST_ORDER_DATE') )
          ) 
cust_df.display()



# COMMAND ----------

# MAGIC %md
# MAGIC ###### Other ways

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 2: Using Spark SQL Temporary View
# MAGIC
# MAGIC You can also use raw SQL directly within PySpark by registering your DataFrame as a temporary view.

# COMMAND ----------

from pyspark.sql.functions import expr, col, count

# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame
raw_emp_df.createOrReplaceTempView("employee")

# Run the SQL query using spark.sql()
sql_query = """

;
"""
result_df = spark.sql(sql_query)

# Show the result
result_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####11: Most selling Product
# MAGIC ----------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ##### SQL query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT PRODUCT_NAME, SUM(QUANTITY) AS TOTAL_QUANTITY
# MAGIC FROM dev.spark_db.customers
# MAGIC --FROM dev.spark_db.employee
# MAGIC GROUP BY product_name
# MAGIC ORDER BY TOTAL_QUANTITY DESC
# MAGIC LIMIT 1
# MAGIC ;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 1: Using DataFrame API (Recommended)
# MAGIC This approach is the most common and "PySpark way" to achieve the result.

# COMMAND ----------

from pyspark.sql.functions import col, expr, max, count, min, sum
from pyspark.sql.types     import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

cust_df = ( raw_cust_df.groupBy("PRODUCT_NAME")
                       .agg(sum('quantity').alias("TOTAL_QUANTITY"))
                       .orderBy(col("TOTAL_QUANTITY").desc())
                       .limit(1)
          ) 
cust_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###### Other ways

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 2: Using Spark SQL Temporary View
# MAGIC
# MAGIC You can also use raw SQL directly within PySpark by registering your DataFrame as a temporary view.

# COMMAND ----------

from pyspark.sql.functions import expr, col, count

# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame
raw_emp_df.createOrReplaceTempView("employee")

# Run the SQL query using spark.sql()
sql_query = """

;
"""
result_df = spark.sql(sql_query)

# Show the result
result_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####12: Get total revenue and number of orders per region
# MAGIC ----------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ##### SQL query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT region
# MAGIC       ,COUNT(c.ORDER_ID) as ORDR_CNT
# MAGIC       ,SUM(quantity * price) as SALES
# MAGIC FROM dev.spark_db.customers as c
# MAGIC JOIN dev.spark_db.orders as o on c.order_id = o.order_id
# MAGIC GROUP BY region
# MAGIC ;
# MAGIC
# MAGIC --FROM dev.spark_db.employee

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 1: Using DataFrame API (Recommended)
# MAGIC This approach is the most common and "PySpark way" to achieve the result.

# COMMAND ----------

from pyspark.sql.functions import col, expr, max, count, min, sum
from pyspark.sql.types     import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

cust_df = spark.table("dev.spark_db.customers").alias('c')
ordr_df = spark.table("dev.spark_db.orders").alias('o')

join_expr = expr("c.order_id== o.order_id")

region_df = ( cust_df.join(ordr_df, join_expr, "inner")
                     .groupBy("region")
                     .agg( count("c.order_id").alias("ORDR_CNT"),
                           expr('sum(quantity * price)').alias('SALES')
                         )
            ) 
region_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###### Other ways

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 2: Using Spark SQL Temporary View
# MAGIC
# MAGIC You can also use raw SQL directly within PySpark by registering your DataFrame as a temporary view.

# COMMAND ----------

from pyspark.sql.functions import expr, col, count

# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame
raw_emp_df.createOrReplaceTempView("employee")

# Run the SQL query using spark.sql()
sql_query = """

;
"""
result_df = spark.sql(sql_query)

# Show the result
result_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####14 Customers with more than 5 orders 
# MAGIC ----------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ##### SQL query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_name, COUNT(ORDER_ID) AS ORDR_CNT
# MAGIC FROM dev.spark_db.customers
# MAGIC --FROM dev.spark_db.employee
# MAGIC GROUP BY customer_name
# MAGIC HAVING ORDR_CNT > 5
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 1: Using DataFrame API (Recommended)
# MAGIC This approach is the most common and "PySpark way" to achieve the result.

# COMMAND ----------

from pyspark.sql.functions import col, expr, max, count, min
from pyspark.sql.types     import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

cust_df = ( raw_cust_df.groupBy("CUSTOMER_NAME")
                       .agg(count("ORDER_ID").alias("ORDR_CNT") )
                       .filter(col("ORDR_CNT") > 5)
          ) 
cust_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Other ways

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 2: Using Spark SQL Temporary View
# MAGIC
# MAGIC You can also use raw SQL directly within PySpark by registering your DataFrame as a temporary view.

# COMMAND ----------

from pyspark.sql.functions import expr, col, count

# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame
raw_emp_df.createOrReplaceTempView("employee")

# Run the SQL query using spark.sql()
sql_query = """

;
"""
result_df = spark.sql(sql_query)

# Show the result
result_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### TILL HERE