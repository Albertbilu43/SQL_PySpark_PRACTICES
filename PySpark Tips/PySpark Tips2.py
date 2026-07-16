# Databricks notebook source
# MAGIC %md
# MAGIC ##Agenda
# MAGIC
# MAGIC ### 3 Pyspark Tips  
# MAGIC      8-   
# MAGIC      9-   join(): Handling duplicate/ambiguous Columns / alias
# MAGIC      9.1- Lateral join, left_semi, left_anti, 
# MAGIC      10-  CTE
# MAGIC      11-  withColumn/withColumns calling a newly created column in the same transformation is NOT ALLOWED
# MAGIC      12-  Renaming a column. Use withCoulumnRenamed/withCoulumnsRenamed
# MAGIC      13-  Remove/Drop a column(s) 
# MAGIC      14-  Use AI to parse and extract the required information from unstructured data
# MAGIC      15-  Nulls s
# MAGIC      16-  Complex Data Types
# MAGIC      17-  Window Functions
# MAGIC      18-  LAG
# MAGIC      19-  Temporary View
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ![image_1774985632190.png](./image_1774985632190.png "image_1774985632190.png")
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---------------------------------------------------------------------
# MAGIC ### HERE PASTE NUMBER 8
# MAGIC ---------------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ------------------------------------------------------------------
# MAGIC #### 9  join(): Handling duplicate/ambiguous columns / alias
# MAGIC ------------------------------------------------------------------
# MAGIC
# MAGIC 1. Joining tables can be done in different ways:
# MAGIC -       Creating a datframe BEFORE tables are LOADED by reading files from Volumens using the FOOL format. 
# MAGIC         spark.read.format.....
# MAGIC
# MAGIC -       Creating a dataframe AFTER the tables were LOADED with data from a file. CALLING THE ALREADY LOADED TABLE DIRECTLY
# MAGIC         spark.table.....
# MAGIC
# MAGIC 3. Handling Duplicate Columns When using a boolean expression (like df1.id == df2.id), both columns will appear in the result, which can cause "ambiguous column" errors later.  
# MAGIC
# MAGIC ###### Fix:
# MAGIC
# MAGIC -       Use alias: A better approach is to assign aliases to the dataframes, and then reference the output columns from the join operation using these aliases:
# MAGIC         https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html
# MAGIC -       Use .drop(df2.id) after the join 
# MAGIC -       use the string format on="id" if the names are identical, as Spark will automatically merge them into one column.
# MAGIC
# MAGIC
# MAGIC ##### Example: Total revenue and number of ordes per region
# MAGIC
# MAGIC Find it in this path:
# MAGIC
# MAGIC spark_programmingHBL / HBL_Practice_Transformations-Query the data / 1 SQL Interviews-customer_product
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Join example 1:   Creating a datframe BEFORE tables are LOADED by reading files from Volumens using the FOOL format. 
# MAGIC   spark.read.format.....
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

"""
from pyspark.sql.functions import expr, col, count, max, sum

result_df =  ( raw_cust_df.alias('C').join(raw_ord_df.alias('O'), col("C.ORDER_ID") == col("O.ORDER_ID"), how="inner")
                                     .groupBy("region")
                                     .agg( sum(expr("quantity * price")).alias("total_revenue"), count(col("C.ORDER_ID")).alias("order_count"))
                                     .orderBy(col("total_revenue").desc() )

             )

result_df.display()
"""


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC FGind it in t6his path: spark_programming /CH07-Spark Joins / 02- Inner Joins
# MAGIC
# MAGIC ###### Join ecxample 2: Creating a dataframe AFTER the tables were LOADED with data from a file. CALLING THE ALREADY LOADED TABLE DIRECTLY
# MAGIC   spark.table.....
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import expr, col

# Here we are NOT using the spark.read. FOOL format, instead we call the tables, which now contain data, directly spark.table...
members_df = spark.table("dev.spark_db.members").alias("m") 
bookings_df = spark.table("dev.spark_db.bookings").alias("b")

#join_expr = expr("m.menber_id == b.member_id")
join_expr = col("m.member_id") == col("b.member_id") # Here we save the join condition/expression in a variable

reports_df = (
    members_df.join(bookings_df, join_expr, "inner") # Here we use teh join condition/expression variable
        .filter("m.last_name == 'Smith' and b.slots > 5")
        .select("m.member_id", "m.first_name", "m.last_name", "b.facility_id", "b.slots", "b.start_time")
        .orderBy("m.first_name", col("b.slots").desc())
)

reports_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Join example 3: 

# COMMAND ----------

from pyspark.sql.functions import col, expr

bookings_df = spark.table("dev.spark_db.bookings").alias("b")
members_df = spark.table("dev.spark_db.members").alias("m")
facilities_df = spark.table("dev.spark_db.facilities").alias("f")

report_df = (bookings_df.join(members_df, expr("b.member_id == m.member_id"), "inner")
                        .join(facilities_df, col("b.facility_id") == col("f.facility_id"), "inner")
                        .filter("m.last_name == 'Smith' and b.slots > 5")
                        .selectExpr("m.member_id", "m.first_name", "m.last_name", "f.facility_name", "b.slots", "b.slots * f.member_cost as booking_amount", "b.start_time")
                        .orderBy(col("m.first_name").asc(),col("booking_amount").desc())
)

report_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---------------------------------------------------------
# MAGIC ###9.1 Lateral join, left_semi, left_anti, 
# MAGIC ---------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Lateral join allows to query right dataframe for each row of the left dataframe.
# MAGIC
# MAGIC A lateral join (also called a correlated join) allows each row from a left DataFrame to be used as input for a subquery or derived table on the right side, meaning the right side can reference columns from the left side
# MAGIC
# MAGIC
# MAGIC Lateral joins are especially useful when:
# MAGIC 1. You need per-parent Top-N child rows
# MAGIC         e.g. Say the parent record is the left df and you want onw or two top records from the right df
# MAGIC 2. You want to invoke TVFs(Table Value Functions) with arguments derived from each row
# MAGIC   
# MAGIC ###### notebook 04- Later Joins  
# MAGIC ---------------------------------------------------------------

# COMMAND ----------

spark.version

from pyspark.sql.functions import col, expr

data = [ ("inner", "INNER JOIN", "Keeps only rows with matching keys in both DataFrames. "), 
        ("left, leftouter, left_outer", "LEFT OUTER JOIN ", "Keeps all rows from the left DataFrame, and matched columns from the right. Missing matches get null. "),
        ("right, rightouter, right_outer", "RIGHT OUTER JOIN", " Keeps all rows from the right DataFrame, and matched columns from the left. Missing matches get null. "),
        ("outer, full, fullouter, full_outer", "FULL OUTER JOIN", "Keeps all records from both sides. Fills unmatched rows with null. "),
        ("left_semi, semi", "None (Often EXISTS)", "Returns only left DataFrame columns for rows that have a matching key on the right. "),
        ("left_anti, anti", "None (Often NOT EXISTS)", "Returns only left DataFrame columns for rows that do not have a match on the right. "),
        ("cross", "CROSS JOIN", "Generates a Cartesian product (pairs every row of the left table with every row of the right table")
       ]

# Create DataFrame
df = spark.createDataFrame(data, ["PySpark_String", "LiteralsEquivalent", "SQL_ClauseDescription"])
df2 = df.selectExpr("PySpark_String", "LiteralsEquivalent", "SQL_ClauseDescription")
df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC --------------------------------------------------------------
# MAGIC #### 10 CTE
# MAGIC --------------------------------------------------------------
# MAGIC
# MAGIC How to Use CTEs in PySparkTo use CTEs in PySpark, you must register a DataFrame as a temporary view, then use spark.sql() to execute the query.
# MAGIC
# MAGIC ##### If your SQL Query contains a CTE is better to use a temporaryView to replicate that code in Pyspark
# MAGIC
# MAGIC #### Example
# MAGIC

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



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ------------------------------------------------------
# MAGIC #### 11  WithColumn/WithColumns Calling a newly created column in the same transformation
# MAGIC ------------------------------------------------------

# COMMAND ----------

"""from pyspark.sql.functions import expr, col, count, min, max, sum, avg, lag
#from pyspark.sql.window import Window

# Although two changes are happening is considered ONE tramsformation "withColumns" cause even though two transformations are happening both are INSIDE the withcolumns transformation
result_df = ( raw_cust_df.withColumns({ "Newprice": col("price") * 1.1,  # Creating a new column 'Newprice'
                                        "Price": expr("case when order_date < '2022-01-01' then price else Newprice end") # Calling the newly cretaed column 'Newprice' inthe same transformations yields an error
                                     })                  
            ) 

result_df.display()
"""


# COMMAND ----------

# MAGIC %md
# MAGIC ---------------------------------------------------------------
# MAGIC ### Error
# MAGIC [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `Newprice` cannot be resolved.

# COMMAND ----------

# MAGIC %md
# MAGIC ---------------------------------------------------
# MAGIC #### Use two separate transformations INSTEAD
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import expr, col, count, min, max, sum, avg, lag
#from pyspark.sql.window import Window

result_df = ( raw_cust_df.withColumn( "Newprice", col("price") * 1.1)
                         .withColumn( "Price", expr("case when order_date < '2022-01-01' then price else Newprice end") ) # Calling the newly cretaed column 'Newprice' inthe separate transformations is fine                              
            )

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC -----------------------------------------------------------
# MAGIC #### 12- Renaming a column. Use withCoulumnRenamed/withCoulumnsRenamed
# MAGIC -----------------------------------------------------------
# MAGIC      

# COMMAND ----------

from pyspark.sql.functions import expr, col, count, min, max, sum, avg, lag
#from pyspark.sql.window import Window

result_df = ( raw_cust_df.withColumnRenamed( "productid", "product" )    
            ) 

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------------------------
# MAGIC #### 13  Remove/Drop a column(s)
# MAGIC -----------------------------------------------------------

# COMMAND ----------

from pyspark.sql.functions import expr, col, count, min, max, sum, avg, lag
#from pyspark.sql.window import Window

result_df = ( raw_cust_df.drop( "productid") # to drop multiple columnns just add a ',' a comma between the column names    
            ) 

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------------------------
# MAGIC ####  14- Use AI to parse and extract the required information from unstructured data
# MAGIC -----------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC See notebook: 05-Transforming Unstructured data

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------------------------
# MAGIC #### 15 Nulls
# MAGIC
# MAGIC Check the:
# MAGIC
# MAGIC Nulls Folder: CH06-Working with Data Types
# MAGIC
# MAGIC ###### Notebook: 01-Working with nulls
# MAGIC
# MAGIC
# MAGIC ##### Examples:
# MAGIC
# MAGIC ###### Filter rows where a specific column is null
# MAGIC 1)      df.filter( col("column_name").isNull( ) ).show()
# MAGIC
# MAGIC ###### Filter rows where ANY of the specified columns are null
# MAGIC 2)      df.filter( col("col1").isNull( ) | col("col2").isNull( ) ).show()
# MAGIC
# MAGIC
# MAGIC ###### With a dataframe field
# MAGIC 3)      df.filter(df.departmentid.isNull())
# MAGIC
# MAGIC
# MAGIC
# MAGIC ###### Drop or Fill Null Values
# MAGIC Once you find the nulls, you can handle them using the DataFrame.na functions:Drop rows: 
# MAGIC
# MAGIC 1)      df.na.drop() drops any row containing a null value. 
# MAGIC 2)      df.na.fill("Unknown") Fill values: replaces null strings with "Unknown".
# MAGIC
# MAGIC -----------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------------------------
# MAGIC #### 16  Complex Data Types
# MAGIC -----------------------------------------------------------
# MAGIC - Folder: CH06-Working with Data Types
# MAGIC - Notebook: 06-Working with complex data types
# MAGIC
# MAGIC Complex Data Types in Spark
# MAGIC
# MAGIC     Struct
# MAGIC     Array
# MAGIC     Map
# MAGIC     VARIANT
# MAGIC
# MAGIC In PySpark, Array, Struct, and Map are traditional complex data types used to handle nested data, while VARIANT is a newer, high-performance type introduced in Spark 4.0 (and widely used in Databricks) for semi-structured data like JSON.
# MAGIC
# MAGIC     1.ArrayTypeAn ArrayType stores a sequence of elements, all of which must have the same data type.
# MAGIC     -   Best for: Lists of similar items (e.g., tags, transaction IDs).
# MAGIC     -   Key Operations:explode(): Transforms each element of an array into a new row.array_contains(): Checks if a specific value exists in the array.size(): Returns the number of elements.
# MAGIC     
# MAGIC     2.StructTypeA StructType is a collection of StructFields, where each field has a name and its own specific data type.
# MAGIC     -   Best for: Grouping related but different types of data (e.g., an address struct containing street (string) and zip (integer)).
# MAGIC     -   Key Operations:Access fields using dot notation (e.g., df.select("address.city")).Use inline() to flatten an array of structs into multiple columns.
# MAGIC     
# MAGIC     3.MapTypeA MapType stores key-value pairs. All keys must be the same type, and all values must be the same type.
# MAGIC     -   Best for: Flexible schemas where you don't know all the keys in advance (e.g., user-defined attributes).
# MAGIC     -   Key Operations:create_map(): Creates a map from existing columns.map_keys() / map_values(): Extracts all keys or values as an array.
# MAGIC     
# MAGIC     4.VARIANT (New in Spark 4.0)The VariantType is a specialized binary format designed to store semi-structured data (like JSON) more efficiently than a string or a complex Map/Struct.
# MAGIC     -   Best for: High-performance ingestion of raw JSON where the schema is unknown or constantly changing.
# MAGIC     -   Key Operations:to_variant_object(): Converts nested inputs (arrays/maps/structs) into a Variant.variant_get(): Extracts specific fields using path expressions (e.g., $.data).schema_of_variant():
# MAGIC         Infers the SQL schema from a variant column.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1778612677944.png](./image_1778612677944.png "image_1778612677944.png")

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------------------------
# MAGIC #### 17 Window Functions
# MAGIC -----------------------------------------------------------
# MAGIC It is not allowed to use a window function inside an aggregate function. Please use the inner window function in a sub-query.
# MAGIC
# MAGIC ##### Core Anatomy of a Window Function
# MAGIC Every window operation in PySpark requires building a WindowSpec using the pyspark.sql.Window class. 
# MAGIC
# MAGIC It consists of three structural pillars:
# MAGIC 1)      partitionBy(): Groups rows into subsets based on one or more columns (similar to GROUP BY in SQL).
# MAGIC 2)      orderBy(): Sorts the rows within each partition to define a sequence.
# MAGIC 3)      Frame Specification (rowsBetween or rangeBetween): Defines the boundaries of the calculation relative to the 
# MAGIC         current row (e.g., row-based offsets or range-based value boundaries).
# MAGIC
# MAGIC
# MAGIC ##### 1. Ranking Functions
# MAGIC These functions evaluate positions within a partition based on the sorting sequence.
# MAGIC - row_number(): Assigns a sequential unique integer starting from 1.
# MAGIC - rank(): Assigns a rank, but leaves gaps in numbers if values tie.
# MAGIC - dense_rank(): Assigns a rank without skipping numbers on a tie.

# COMMAND ----------

"""
# Define window spec
ranking_window = Window.partitionBy("Department").orderBy(F.desc("Salary"))

# Apply ranking
df.withColumn("RowNumber", F.row_number().over(ranking_window)) \
  .withColumn("Rank", F.rank().over(ranking_window)) \
  .withColumn("DenseRank", F.dense_rank().over(ranking_window)) \
  .show()
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Analytical (Value) Functions
# MAGIC These functions fetch specific values relative to the position of your current row.
# MAGIC - lag(col, offset): Looks backward to pull a value from N rows prior.
# MAGIC - lead(col, offset): Looks forward to pull a value from N rows ahead

# COMMAND ----------

"""
# Define window spec ordered by Salary
analytic_window = Window.partitionBy("Department").orderBy("Salary")

# Fetch previous and next salaries
df.withColumn("PrevSalary", F.lag("Salary", 1).over(analytic_window)) \
  .withColumn("NextSalary", F.lead("Salary", 1).over(analytic_window)) \
  .show()

"""

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Aggregate Window Functions & Frames
# MAGIC You can apply standard aggregates (sum, avg, min, max) over a window. To build running totals or moving averages, you must explicitly bounded your frame.
# MAGIC - Window.unboundedPreceding: The first row of the partition.
# MAGIC - Window.currentRow: The row currently being processed.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Example 1

# COMMAND ----------

"""
# Cumulative Running Total Spec
running_total_window = Window.partitionBy("Department") \
                             .orderBy("Salary") \
                             .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df.withColumn("RunningTotal", F.sum("Salary").over(running_total_window)).show()

"""

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Example 2

# COMMAND ----------

windowSpec = Window.orderBy(col("quantity") * col("price")).rowsBetween(Window.unboundedPreceding, Window.currentRow)

cust_df = ( raw_cust_df.withColumns({ "REVENUE": col("quantity") * col("price"),
                                      "CUMU_REV" : sum(col("quantity") * col("price")).over(windowSpec)
                                    })
                       .select("ORDER_DATE","REVENUE", "CUMU_REV")
          ) 
cust_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Example 3

# COMMAND ----------

# Assuming raw_cust_df is your source DataFrame loaded from dev.spark_db.customers
# Step 1: Calculate revenue and aggregate
t1 = raw_cust_df.groupBy("QUANTITY", "PRICE") \
                .agg(expr("QUANTITY * PRICE AS REV")) \
                .groupBy("REV") \
                .agg(sum("REV").alias("REVENUE"))

# Step 2: Define a window for the cumulative sum
window_spec = Window.orderBy("REVENUE")

# Step 3: Apply the cumulative sum over the window
result = t1.withColumn("CUMU_REV", sum("REVENUE").over(window_spec)) \
           .select("REVENUE", "CUMU_REV")

result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### See folder CH08-Spark-Aggregates
# MAGIC
# MAGIC notebook: 04-Window Aggregate
# MAGIC
# MAGIC ###### Structure

# COMMAND ----------





from pyspark.sql.window    import Window        # Import window function
from pyspark.sql.functions import rank, col     # Import rank, and more functions


----------------------GENERAL STRUCTURE-----------------------
"""
  agg_function().OVER(Window.PARTITION_BY(column_list)
                            .ORDER_BY(column_list)
                            .ROWS_BETWEEN(window_start, window_end)
-----------------------------------------
"""

----------------------OPTION ONE: Use the window function directly in the transformation ----------------------


----------------------OPTION TWO: Save the window function to a variable, then use it in ----------------------
                                  the transformations in the result_df

# Create the window. save it to a variable, then use it in the result_df
"""
window_spec = (
    Window.partitionBy("booked_by")
        .orderBy(col("revenue").desc())
)

result_df = (
    booking_summary_df.withColumn("rank", rank().over(window_spec)) # Use the variable here
            .where("rank <= 3")
            .drop("rank")
)

result_df.display()
"""


# COMMAND ----------

# MAGIC %md
# MAGIC ##### 17.2 TBD

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------------------------
# MAGIC #### 18 
# MAGIC -----------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------------------------
# MAGIC #### 19 Temporary View
# MAGIC ##### Register a temporary View
# MAGIC -----------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Example:
# MAGIC ###### 1 Find duplicates
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ###### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT   customerid, count(*)
# MAGIC FROM     dev.spark_db.customers as C
# MAGIC GROUP BY customerid
# MAGIC HAVING   count(*) > 1

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark
# MAGIC
# MAGIC Method 1: Using DataFrame API (Recommended)
# MAGIC
# MAGIC Method 2: Using a TemporaryView and Spark SQL
# MAGIC
# MAGIC -------------------------------------------------------

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
# MAGIC -----------------------------------------------------------
# MAGIC #### 20 TBD
# MAGIC -----------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------------------------
# MAGIC #### 21 TBD
# MAGIC -----------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------------------------
# MAGIC #### 22 TBD
# MAGIC -----------------------------------------------------------

# COMMAND ----------

