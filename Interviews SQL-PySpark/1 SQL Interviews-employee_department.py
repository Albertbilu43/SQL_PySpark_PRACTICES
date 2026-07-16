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
# MAGIC                                          #"order_id": col("order_id").try_cast(IntegerType())  try_cast returns null if an error hapens  
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
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
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

# COMMAND ----------

# MAGIC %md
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
from pyspark.sql.functions import to_date, col, expr, try_to_date, regexp_replace

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
# MAGIC ##### <> using spark.read.format (FOOL)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC  <> create a Dataframe by reading a file (in the Volume a csv, json, etc.) 
# MAGIC -     using spark.read.format
# MAGIC                       .option
# MAGIC                       .option
# MAGIC                       .load         

# COMMAND ----------


raw_emp_df = ( spark.read.format('csv')
                          .option("header", True)
                          .option("inferSchema", True)
                          .load("/Volumes/dev/spark_db/datasets/spark_programming/data/employee.csv")
              )

raw_emp_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### 2.1 Eyeball the data to spot possible issues

# COMMAND ----------

# MAGIC %md
# MAGIC ------------------------------------------------------------------
# MAGIC #####2.2 Fix any data issues discovered.
# MAGIC After reading the data we noticed that one issue needs to be solved.

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

fix_emp_df= raw_emp_df.withColumns({ "departmentid": col("departmentid").try_cast(IntegerType())                                       
                                     #,"enddate": to_date(col("enddate"), "yyyy-MM-dd")
                                   }
                                  )
fix_emp_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### DEPARETMENT DF

# COMMAND ----------

raw_dep_df = ( spark.read.format('csv')
                          .option("header", True)
                          .option("inferSchema", True)
                          .load("/Volumes/dev/spark_db/datasets/spark_programming/data/department.csv")
              )

raw_dep_df.display()

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
# MAGIC #### 10-50 SQL Interview Questions 
# MAGIC
# MAGIC https://www.linkedin.com/feed/update/urn:li:activity:7402591561454383104/?updateEntityUrn=urn%3Ali%3Afs_updateV2%3A%28urn%3Ali%3Aactivity%3A7402591561454383104%2CFEED_DETAIL%2CEMPTY%2CDEFAULT%2Cfalse%29
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC
# MAGIC #### Second highest salary by employee
# MAGIC ------------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MAX(SALARY) as SECOND_TOP_SALARY
# MAGIC FROM   dev.spark_db.employee as 
# MAGIC WHERE SALARY < (SELECT MAX(SALARY) FROM dev.spark_db.employee)

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

# MAGIC %md
# MAGIC #### Option One

# COMMAND ----------


from pyspark.sql.functions import to_date, col, expr, sum, count, max 

max_salary = raw_emp_df.agg(max("salary")).first()[0] # max_salary

# Then filter for salaries less than max and get the second highest
result_df = (raw_emp_df.filter(col("salary") < max_salary)
                       .agg(max("salary").alias("SECOND_TOP_SALARY"))
            )

result_df.display()

# First, get the maximum salary
"""Esta línea de código se utiliza comúnmente en
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
# MAGIC ##error:
# MAGIC ##### This piece of code below will produce an error cause 
# MAGIC
# MAGIC you cannot replicate the query directly using a selectExper() as it requires the registration of the dataframe as a temporary view first
# MAGIC
# MAGIC
# MAGIC Use the standard way instead

# COMMAND ----------

"""
from pyspark.sql.functions import to_date, col, expr, sum, count

result_df =(  raw_cust_df.selectExpr("productid" ,"sum(quantity * price) as Revenue")
                         .orderBy("Revenue", ascending=False)
                         .limit(3)
           )
            
result_df.display()
"""       

# COMMAND ----------

# MAGIC %md
# MAGIC #### Option Two
# MAGIC
# MAGIC Creating a temporary view

# COMMAND ----------

from pyspark.sql.functions import expr, col, count

# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame
raw_emp_df.createOrReplaceTempView("employee")

# Run the SQL query using spark.sql()
sql_query = """
SELECT MAX(SALARY) as SECOND_TOP_SALARY
FROM employee
WHERE SALARY < (SELECT MAX(SALARY) FROM employee)
"""
result_df_sql = spark.sql(sql_query)

# Show the result
result_df_sql.show()

# COMMAND ----------

"""from pyspark.sql.functions import to_date, col, expr, sum, count


result_df =(  raw_cust_df.groupBy("productid")
                         .agg(expr("sum(quantity * price)  as Revenue"))
                         .orderBy("Revenue", ascending=False)
                         .limit(3)
           )
            
result_df.display()
"""


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
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### Find employees without department
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT EMPLOYEE_ID
# MAGIC FROM dev.spark_db.employee as e
# MAGIC WHERE DEPARTMENTID IS NULL

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option One
# MAGIC ###### Standard way

# COMMAND ----------


from pyspark.sql.functions import col, expr, regexp_replace

result_emp_df =(  raw_emp_df.where(col("departmentid").isNull())
                            .select(col("employee_id"))
               ) 
result_emp_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Creating a temporaryview

# COMMAND ----------

from pyspark.sql.functions import col, expr

#Create a temporaryView
raw_emp_df.createOrReplaceTempView("employee")

sql_query= """
          SELECT EMPLOYEE_ID
          FROM dev.spark_db.employee as e
          WHERE DEPARTMENTID IS NULL
           """

result_emp_df= spark.sql(sql_query)
result_emp_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC ### TBD

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### NUMBER TBD
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 1: Using DataFrame API (Recommended)
# MAGIC This approach is the most common and "PySpark way" to achieve the result.

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
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
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### 1 TBD
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark

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
# MAGIC ######Method 2: Using Spark SQL
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
# MAGIC ### Practica #1: Number of distint products types

# COMMAND ----------

# MAGIC %md
# MAGIC #### With SQL query

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

# COMMAND ----------

# MAGIC %md
# MAGIC #### SQL query

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

