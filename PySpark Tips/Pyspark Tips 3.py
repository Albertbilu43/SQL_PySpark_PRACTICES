# Databricks notebook source
# MAGIC %md
# MAGIC ##Agenda
# MAGIC
# MAGIC ### Pyspark Tips  
# MAGIC
# MAGIC DataFrame
# MAGIC
# MAGIC      1-   collect
# MAGIC      2-   dropduplicates
# MAGIC      3-   subqueries - scalar
# MAGIC      4-   GroupBy errors
# MAGIC
# MAGIC Functions
# MAGIC
# MAGIC      1-   split
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ## DataFrames
# MAGIC --------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ----------------------------------------------------------------
# MAGIC ###1-   .collect
# MAGIC
# MAGIC ##### Returns all the records in the DataFrame as a list of Row
# MAGIC
# MAGIC ##### Notes: USE IT only if the resulting list is expected to be small, as all the data is loaded into the driver’s memory!!!!!!
# MAGIC
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.collect.html
# MAGIC
# MAGIC https://www.linkedin.com/pulse/master-pyspark-understanding-collect-examples-reddy-molakatalla-nvwcc/

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('PySparkCollectExample').getOrCreate()

dept = [("Finance", 10), 
        ("Marketing", 20), 
        ("Sales", 30), 
        ("IT", 40)]
deptColumns = ["dept_name", "dept_id"]

deptDF = spark.createDataFrame(data=dept, schema=deptColumns)
deptDF.show(truncate=False) 

# COMMAND ----------

#Returns value of First Row, First Column which is "Finance"
deptDF.collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC ---------------------------------------------------
# MAGIC ### 2 dropDuplicates
# MAGIC
# MAGIC ##### df.dropDuplicates() method in PySpark returns a new DataFrame with duplicate rows removed. 
# MAGIC ###### Unlike distinct(), which checks entire rows, dropDuplicates() allows you to pass a subset of specific columns to target for deduplication.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Drop Duplicates Across All Columns 
# MAGIC
# MAGIC     When called without arguments, it drops rows where every single column is identical.

# COMMAND ----------

# Removes exact duplicate rows
df_unique = df.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Drop Duplicates Based on Specific Columns
# MAGIC
# MAGIC     Pass a list of column names to deduplicate based on only those fields

# COMMAND ----------

# Keeps only the first encountered row for each unique customer_id
df_unique = df.dropDuplicates(["customer_id"])

# Deduplicates based on a combination of columns
df_unique = df.dropDuplicates(["customer_id", "order_date"])


# COMMAND ----------

# MAGIC %md
# MAGIC ---------------------------------------------------
# MAGIC ### 3 subqueries - scalar
# MAGIC
# MAGIC ######  DataFrame.scalar() 
# MAGIC extracts a single-value computation or aggregation from a DataFrame and returns it as a Column object representing a scalar subquery.
# MAGIC ###### Key Benefits: 
# MAGIC 1)      No collect() needed: Keeps the operation entirely within the Spark engine, avoiding the performance penalties of bringing data to the local driver.
# MAGIC
# MAGIC 2)      Inline Subqueries: Allows you to perform tasks like comparing row values against a global or grouped average natively inside your operations

# COMMAND ----------

# Using Scalar Subqueries .scalar(). 

Example 1

# A scalar subquery returns exactly one row and one column. Use .scalar() to get this single value to filter or calculate columns on an outer 
total_rev = (raw_cust_df.selectExpr(' sum(quantity * price)')  ) # total_rev is  a DataFrame with one column and one row but you need the value only so .scalar() is needed
         
cust_df = ( raw_cust_df.groupBy("product_name")
                       .agg(sum(col("quantity") * col("price")).alias("PROD_REVENUE"),
                            expr("sum(quantity * price)" ) * 100 / (total_rev.scalar())  # Use .scalar() to get this single value, from the total_rev dataframe, to filter or calculate columns on an outer DataFrame                                               
                            )
           )

cust_df.display()

Example 2

"""
# Extract global average salary as a scalar column reference
avg_salary_scalar = employees.select(sf.avg("salary")).scalar()

# Use the scalar column in a filter condition
high_earners = employees.where(sf.col("salary") > avg_salary_scalar)
high_earners.select("name", "salary").show()
"""


# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------
# MAGIC ####4- GroupBy  Errors

# COMMAND ----------

# MAGIC %md
# MAGIC  In PySpark:
# MAGIC
# MAGIC     After .groupBy(), you get a GroupedData object
# MAGIC     GroupedData only supports aggregation methods like .agg(), .count(), etc.
# MAGIC     .withColumn() can only be called on a DataFrame
# MAGIC
# MAGIC In the example below The second groupBy is unnecessary. The window function should be applied directly to t1 as a DataFrame, not after a second grouping.

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRONG

# COMMAND ----------

t1 = ( raw_cust_df.withColumn('YR', year('order_date'))
                  .groupBy('YR')      
                  .agg( expr('sum(quantity * price) as REVENUE') )
                  .select('YR', 'REVENUE')
     ) 

windowSpec = ( Window.orderBy('YR') )

cust_df = (  t1.GroupBy('YR', 'REVENUE ') -------------------------------------------------> tHIS groupBy is unnescesary!!!
               .withColumn('YoY_GROWTH', col('REVENUE') - lag('REVENUE').over(windowSpec))
          )

cust_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### CORRECT

# COMMAND ----------

t1 = ( raw_cust_df.withColumn('YR', year('order_date'))
                  .groupBy('YR')      
                  .agg( expr('sum(quantity * price) as REVENUE') )
                  .select('YR', 'REVENUE')
     ) 

windowSpec = ( Window.orderBy('YR') )

cust_df = (  t1.withColumn('YoY_GROWTH', col('REVENUE') - lag('REVENUE').over(windowSpec))
          )

cust_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC -------------------------------------------------------------
# MAGIC ## Functions
# MAGIC ------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ###1 split
# MAGIC
# MAGIC ##### Divides a string column into an ArrayType column based on a specified delimiter or regular expression pattern
# MAGIC
# MAGIC functions.split(str, pattern, limit=- 1)
# MAGIC
# MAGIC str Column or column name
# MAGIC
# MAGIC     a string expression to split
# MAGIC
# MAGIC pattern Column or literal string
# MAGIC
# MAGIC     a string representing a regular expression. The regex string should be a Java regular expression.
# MAGIC
# MAGIC limit Column or column name or int
# MAGIC
# MAGIC     an integer which controls the number of times pattern is applied.
# MAGIC
# MAGIC         limit > 0: The resulting array’s length will not be more than limit, and the
# MAGIC
# MAGIC             resulting array’s last entry will contain all input beyond the last matched pattern.
# MAGIC
# MAGIC         limit <= 0: pattern will be applied as many times as possible, and the resulting
# MAGIC
# MAGIC             array can be of any size.
# MAGIC
# MAGIC ##### Returns
# MAGIC
# MAGIC     Column
# MAGIC
# MAGIC         array of separated strings.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### option One
# MAGIC ##### Be CAREFUL with teh use of .getItem() !!!!
# MAGIC
# MAGIC IF ANY OF THE DATAAFRAME RECORDS HAS ONLY ONE ELEMENT, IT WILL FAIL 
# MAGIC ###### ex.  if  (3, "Bob Spark")  is  (3, "Bob") !!!!
# MAGIC
# MAGIC cause the getItem(1) is looking for a 1 index position , which in this case is missing there
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, split

# Initialize SparkSession
spark = SparkSession.builder.appName("SplitExample").getOrCreate()

# Sample data: Full names separated by a space. 

data = [
    (1, "John Doe"),
    (2, "Jane Mary Smith"),
    (3, "Bob Spark")   
]

# Create DataFrame
df = spark.createDataFrame(data, ["id", "full_name"])

# 1. Basic Split: Converts the string into an Array column
# We add a  new column named 'name_array' which is teh result of splitting the full_name column by space
df_array = df.withColumn("name_array", split(col("full_name"), " "))
#df_array.display()

# 2. Extract Elements: Pull specific array items using getItem()
df_final = df_array \
            .withColumn("first_name", col("name_array").getItem(0)) \
            .withColumn("last_name" , col("name_array").getItem(1))

# Show the results
df_final.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC Avoid the missing index error by using teh get() method instead of getItem()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, expr

# Initialize SparkSession
spark = SparkSession.builder.appName("SplitExample").getOrCreate()

# Sample data: Full names separated by a space
data = [
    (1, "John Doe"),
    (2, "Jane Mary Smith"),
    (3, "Bob")
]

# Create DataFrame
df = spark.createDataFrame(data, ["id", "full_name"])

# 1. Basic Split: Converts the string into an Array column
df_array = df.withColumn("name_array", split(col("full_name"), " "))

# 2. Extract Elements: Pull specific array items using get() which handles out-of-bounds gracefully
from pyspark.sql.functions import get

df_final = df_array \
    .withColumn("first_name", get(col("name_array"), 0)) \
    .withColumn("last_name", get(col("name_array"), 1))

# Show the results
df_final.show(truncate=False)

# COMMAND ----------

