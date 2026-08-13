# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Agenda
# MAGIC
# MAGIC ### 3 Pyspark Tips  
# MAGIC      1-   Methods for dataframes(read, format, etc, )
# MAGIC      2-   Select() vs selectExpr() vs expr()
# MAGIC           -WARNING: col() cannot be nested inside expr()
# MAGIC           -Renaming a Column (Alias) vs AS
# MAGIC               Use alias() with "select(col().alias())" 
# MAGIC               Use  'AS'   with "selectExpr()"
# MAGIC           -WARNING 'where' SQL keyword CANNOT be used inside selectExpr()
# MAGIC
# MAGIC      3-   Grouping vs agg()
# MAGIC           3.1 .agg(max("column")).first()[0] # max_value
# MAGIC      4-   where/filter/having
# MAGIC      5-   regexp_replace(), replace()
# MAGIC      6-   cast(), try_cast()
# MAGIC      7-   Dates (datediff, interval)
# MAGIC
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
# MAGIC
# MAGIC
# MAGIC -----------------------------------------------------------
# MAGIC #### 1 Methods for dataframes(read, format, etc)
# MAGIC -----------------------------------------------------------
# MAGIC       spark.read.  Returns a DataFrameReader  to read data in a DataFrame. 
# MAGIC       https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.read.html
# MAGIC
# MAGIC       spark.read.format Specifies the file type (can be "json", "parquet", "jdbc", "orc", "text", etc.)..
# MAGIC       https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.format.html#
# MAGIC
# MAGIC       spark.read.option  Adds an input option for the underlying data source.
# MAGIC       https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.option.html
# MAGIC
# MAGIC       spark.read.load   Loads the data from the specified path(s)
# MAGIC       https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.load.html
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC -----------------------------------------------------------
# MAGIC #### 2 select() vs selectExpr()
# MAGIC
# MAGIC ###### Both methods return a new DataFrame and are used for selecting and transforming columns
# MAGIC -----------------------------------------------------------
# MAGIC
# MAGIC
# MAGIC #####< The core difference is that>
# MAGIC
# MAGIC #### select( *cols)
# MAGIC * select() uses PySpark's Column API with expressions(column objects and functions) or raw column name strings. 
# MAGIC ###### DataFrame.select( *cols) 
# MAGIC       Where Parameters: cols: str, Column, or list
# MAGIC              column names (string) or expressions (Column). 
# MAGIC              If one of the column names is ‘ * ’, that column is expanded to include all columns in the current DataFrame.
# MAGIC
# MAGIC        
# MAGIC #### selectExpr() 
# MAGIC * Accepts standard SQL expressions as strings. 
# MAGIC          but CAREFUL you CANNOT include 'where' statement inside it or do any groupings for a particular dimension
# MAGIC          UNLESS you create a TemporaryView first to execute a whole SQL expresion
# MAGIC
# MAGIC -----------------------------------------------------------------------------------------
# MAGIC
# MAGIC select() Key Characteristics: 
# MAGIC
# MAGIC   -  API: Uses Column objects and Column expressions (col("...")).
# MAGIC   -  Use Case: Ideal for selecting specific columns and basic manipulation.
# MAGIC   -  Example: Select column  objetcs --->      df.select(col("name") ) 
# MAGIC   -  Example: select columns  names  --->      df.select("name")
# MAGIC   -  Example: select df[]     names  --->      df.select(df["name"])
# MAGIC   -  Example: select df.colum names  --->      df.select(df.name)  
# MAGIC
# MAGIC selectExpr() Key Characteristics: 
# MAGIC
# MAGIC   -  API: Accepts only SQL expressions formatted as strings.
# MAGIC   -  Use Case: Ideal for rapid SQL-style transformations and alias naming.
# MAGIC   -  Example: df.selectExpr("name", "age + 10 as age_plus_10")
# MAGIC
# MAGIC Key Differences:
# MAGIC
# MAGIC   -  Syntax: select() uses Pythonic column expressions; selectExpr() uses pure string SQL.
# MAGIC   -  Flexibility: selectExpr() is more concise for complex SQL transformations (e.g., when, abs, concat) in a single string.
# MAGIC   -  Performance: Generally, both are efficient, but select() is slightly more direct as it avoids parsing SQL strings.
# MAGIC   -  Renaming: selectExpr() makes renaming columns within expressions more concise. 
# MAGIC
# MAGIC .
# MAGIC
# MAGIC
# MAGIC
# MAGIC ##### <When to Use Which?>
# MAGIC
# MAGIC ######   Use select() 
# MAGIC     when you are building transformations programmatically (e.g., looping through a list of Column objects) or when you prefer a strictly Pythonic API style.
# MAGIC -------------------------------------------------------------    
# MAGIC ######  Use selectExpr() 
# MAGIC     when you want to write concise, SQL-like transformations without importing the functions module for every simple operation. 
# MAGIC     It is also excellent for users already comfortable with SQL syntax
# MAGIC """
# MAGIC #####   <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<expr() vs. selectExpr()>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# MAGIC
# MAGIC
# MAGIC ##### expr()             
# MAGIC                       operates on a single SQL expression and returns a Column object for use within other DataFrame methods, 
# MAGIC ##### selectExpr()        
# MAGIC                       Accepts one or more SQL expressions as strings and applies them across the entire DataFrame in a single call,
# MAGIC                       returning a new DataFrame. 
# MAGIC
# MAGIC                       <<python>>
# MAGIC                       # Using selectExpr to perform multiple operations at once
# MAGIC                       
# MAGIC                       -  df_transformed = df.selectExpr( "name", "Balance * 1.02 AS Adjusted_Balance",
# MAGIC                                                         "CASE WHEN Balance > 100000 THEN 'High' ELSE 'Low' END AS Balance_Level"
# MAGIC                                                       )
# MAGIC
# MAGIC
# MAGIC #### Key differences
# MAGIC expr() is a standalone function that evaluates a single SQL expression into a column, whereas 
# MAGIC
# MAGIC selectExpr() is a DataFrame method that lets you run multiple SQL expressions to select and transform columns simultaneously
# MAGIC
# MAGIC Feature---------expr()---------------------------------------------------selectExpr()-------------
# MAGIC
# MAGIC Type------------Function (imported from pyspark.sql.functions) -----Method (called directly on a DataFrame)
# MAGIC
# MAGIC Arguments-----Takes exactly one string expression.------------------Takes one or more comma-separated string expressions.
# MAGIC
# MAGIC Usage----------Must be nested inside methods like select(), --------Acts as a standalone drop-in replacement for select().
# MAGIC                 withColumn(), or filter().
# MAGIC
# MAGIC Output---------Returns a Column object.-----------------------------Returns a new DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC #####  col(col)           
# MAGIC                     - Se utiliza para hacer referencia a una columna de un DataFrame por su nombre, devolviendo un objeto de tipo Column. 
# MAGIC                     Es fundamental para:
# MAGIC                     -  transformaciones
# MAGIC                     -  filtros 
# MAGIC                     -  ordenaciones al convertir cadenas de texto en expresiones de columna. 
# MAGIC                        col("nombre_columna").
# MAGIC
# MAGIC #####  column(col)        
# MAGIC                     - Does the sam e as 'col(col)' but 'col(col)' is more preferred. 
# MAGIC #####  lit(col)           
# MAGIC                     - Creates a Column of literal value.
# MAGIC #####  try_cast(dataType) 
# MAGIC                     - A special version of cast that performs the same operation, but returns a NULL value instead of raising 
# MAGIC                       an error if the invoke method throws exception.
# MAGIC """

# COMMAND ----------

"""

The core difference between PySpark's
The core difference is that 
1)    select() -------> accepts column objects or raw column name strings. Uses the DataFrame API's column objects and functions
2)    selectExpr() ---> accepts SQL-style expression strings
3)    expr() ---------> is a function that parses a SQL expression string into a Column object. 
                        Useful when you want to apply a SQL expression to a DataFrame column or perform a SQL-like operation on a DataFrame.

          Example: # 1. Combining Column objects and SQL strings within a standard select()
                      df.select(col("name"), expr("age + 1 as next_year_age"))

          Example: # 2. Using it inside a filter condition
                      df.filter(expr("age > 18 AND status = 'Active'"))

          Example: # 3. Using expr() with sum, max, etc.
             
                        Calculates the total sum of a column
                        df.agg(expr("sum(sales)")).show()

                        Evaluates conditional math inside the sum
                        df.select(expr("sum(price * quantity)")).show() 

          WARNING: col() cannot be nested inside expr() because :
                    expr() expects a raw SQL STRING but returns a Column object, you can use it interchangeably with col() 
                    col() returns a Column object NOT A STRING.
                                  
                    # ❌ THIS WILL FAIL
                      df.select(expr(col("age") + 1))


<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<expr() vs. selectExpr()>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

*expr()              operates on a single SQL expression and returns a Column object for use within other DataFrame methods, 
*selectExpr()        Accepts one or more SQL expressions as strings and applies them across the entire DataFrame in a single call, returning a new
                     DataFrame. 
                     <<python>>
                     # Using selectExpr to perform multiple operations at once
                     df_transformed = df.selectExpr( "name",
                                                     "Balance * 1.02 AS Adjusted_Balance",
                                                      "CASE WHEN Balance > 100000 THEN 'High' ELSE 'Low' END AS Balance_Level"
                                                    )
<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< .col vs. .column >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
col and column are functionally identical. 
Both are functions within the pyspark.sql.functions module that return a Column object based on a given string name.

Convention: 
col is much more commonly used in the PySpark community because it is shorter and more concise

col(col)           - Se utiliza para hacer referencia a una columna de un DataFrame por su nombre, devolviendo un objeto de tipo Column. 
                     Es fundamental para transformaciones, filtros y ordenaciones al convertir cadenas de texto en expresiones de columna. 
                     col("nombre_columna").
column(col)        - Does the sam e as 'col(col)' but 'col(col)' is more preferred. 


lit(col)           - Creates a Column of literal value.
try_cast(dataType) - A special version of cast that performs the same operation, but returns a NULL value instead of raising an error if 
                     the invoke method throws exception.
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ----------------------------------------------------------
# MAGIC #### select() WARNINGS

# COMMAND ----------

# MAGIC %md
# MAGIC -------------------------------------------------------------
# MAGIC #### selectExpr() WARNINGS
# MAGIC                     YOU CANNOT put groupby inside selectExpr() 
# MAGIC                     ❌ THIS WILL FAIL: raw_emp_df.selectExpr(" name, count(*) groupby(name)"))
# MAGIC                     ✅ THIS WILL WORK: raw_emp_df.selectExpr("name").groupBy("name").count()
# MAGIC
# MAGIC ----------------------------------------------------------------------------
# MAGIC                    YOU CANNOT put 'where' SQL keyword inside selectExpr()
# MAGIC                      The selectExpr() method is designed for selecting columns and applying SQL expressions to those 
# MAGIC                      columns(e.g., transformations, aggregations, or conditional logic with CASE WHEN), not to filter the dataset  
# MAGIC
# MAGIC                      ✅ THIS WILL WORK: raw_emp_df.filter(raw_emp_df.departmentid.isNull())
# MAGIC                                                    .selectExpr('name', 'departmentid')
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ----------------------------------------------------------
# MAGIC #### expr() WARNINGS 
# MAGIC
# MAGIC ###### WARNING: col() cannot be nested inside expr()
# MAGIC                     You can HAVE col() and expr() INSIDE  A select() because both return a col object:   df.select(col("name"), expr("age + 1 as next_year_age")) 
# MAGIC                     
# MAGIC                     BUT CANNO PUT col() INSIDE A expr() !!!!!!!!!!!!!!!!!
# MAGIC                     expr() expects a raw SQL STRING but returns a Column object, you can use it interchangeably with col() 
# MAGIC                     col() returns a Column object NOT A STRING.
# MAGIC                                   
# MAGIC                     # ❌ THIS WILL FAIL
# MAGIC                       df.select(expr(col("age") + 1))

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------------------
# MAGIC ##### Examples Renaming a Column (Alias) vs AS

# COMMAND ----------

In select(), you must use a Column object to call .alias(). 
# select()
#from pyspark.sql import functions as F
#df.select(F.col("age").alias("current_age"))

In selectExpr(), you use the SQL 'AS' keyword.
# selectExpr()
#df.selectExpr("age AS current_age")

In expr(), parses a SQL expression string into a Column object, allowing you to write raw SQL logic directly inside DataFrame transformations. 




# COMMAND ----------

# MAGIC %md
# MAGIC ###### Examples Mathematical Operations & SQL Functions 
# MAGIC
# MAGIC selectExpr() Is not designed to filter the dataset 
# MAGIC
# MAGIC is often more concise for simple calculations or applying built-in SQL functions like abs(), upper(), or cast().
# MAGIC
# MAGIC

# COMMAND ----------

# select()
#df.select((F.col("age") + 10).alias("age_plus_ten"))

# selectExpr()
#df.selectExpr("age + 10 AS age_plus_ten", "CAST(id AS STRING)")


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ![image_1774641851028.png](./image_1774641851028.png "image_1774641851028.png")
# MAGIC
# MAGIC -----------------------------------------------------------
# MAGIC #### 3 groupBy() and agg()
# MAGIC -----------------------------------------------------------
# MAGIC
# MAGIC ##### ----> What is GroupBy?
# MAGIC
# MAGIC PySpark groupBy( ) is used to split the data into groups based on one or more columns, which can then be aggregated or transformed independently.
# MAGIC
# MAGIC
# MAGIC Group data
# MAGIC ###### 1- grouped = df.groupBy("department")
# MAGIC
# MAGIC Once data is grouped, you can  apply aggregations .count() or .sum() to obtain a new DataFrame.
# MAGIC
# MAGIC ###### 2- grouped.count().show()
# MAGIC
# MAGIC ##### ----> Parameters and return values
# MAGIC
# MAGIC The only parameter for the method is *cols which accepts:
# MAGIC - column names 
# MAGIC - column expressions
# MAGIC - column ordinals (int) 
# MAGIC - list of columns.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ------------------------------------------------------------------------------
# MAGIC #### 3.1  .agg(max("column")).first()[0] # max_value
# MAGIC -------------------------------------------------------------------------------
# MAGIC
# MAGIC In PySpark, the sequence agg(...).first()[0] is a common pattern used to extract a single scalar value from an aggregated DataFrame.
# MAGIC
# MAGIC max_salary = raw_emp_df.agg(max("salary")).first()[0] # max_salary
# MAGIC
# MAGIC #### max_salary is a Python variable, not a column in the DataFrame. 
# MAGIC #### It doesn't exist in the DataFrame schema so to be able to use it in a dataframe YOU MUST wrap it in a lit() function.
# MAGIC #### The fix is to add max_salary as a literal column using lit():

# COMMAND ----------

""" Breakdown of the Sequence:

    raw_emp_df: Es el nombre del DataFrame que contiene los datos de los empleados.
    .agg(max("salary")):
        agg es la función de agregación.
        max_("salary") calcula el valor máximo de la columna llamada "salary".
        Nota: El resultado de este paso sigue siendo un DataFrame con una sola fila y una sola columna.
    .first()[0]:
        Esta es una "acción" de Spark que toma la primera fila del DataFrame resultante y la devuelve como un objeto tipo Row de Spark
        [0]: Accede al primer elemento de esa fila (el valor numérico del salario máximo). 
        Sin esto, tendrías un objeto Row(max(salary)=5000), pero con el [0] obtienes simplemente el 5000.
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Avg salary example:

# COMMAND ----------

"""
from pyspark.sql.functions import lit

avg_sal= raw_emp_df.agg(avg('salary')).first()[0]   # Extraxt average salary as a scalar value
                                                    #

emp_df = ( raw_emp_df.filter(col('salary') > avg_sal)
                     .select(col('name'), col('salary'), lit(avg_sal).alias('avg_sal') )
          )

emp_df.display()

"""

# COMMAND ----------

# MAGIC %md
# MAGIC ##### DatFrame example data

# COMMAND ----------

from pyspark.sql import Row

data = [
    Row(department="Sales", employee="Alice", salary=5000),
    Row(department="Sales", employee="Bob", salary=4800),
    Row(department="HR", employee="Carol", salary=4000),
    Row(department="HR", employee="David", salary=3900),
    Row(department="IT", employee="Eve", salary=6000)
]

df = spark.createDataFrame(data)
df.show()

# COMMAND ----------

#grouping by column NAME like above
df.groupBy("department")

#grouping by column EXPRESSION
df.groupBy(df.department)

#grouping by column ORDINAL
df.groupBy(1)

#grouping by LIST of COLUMNS, you can mix the methods!
df.groupBy(["department", 2])


# COMMAND ----------

# MAGIC %md
# MAGIC ##### ----> GroupBy on single and multiple columns

# COMMAND ----------

#grouping by SINGLE COLUMN
df.groupBy("department").sum(“salary”).show()

#grouping by MULTIPLE COLUMNS
df.groupBy(["department", 2]).sum(“salary”).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ----> Multiple aggregations with agg()
# MAGIC
# MAGIC We do not need to aggregate on the same column with agg(), you can define a different column for each aggregation function.

# COMMAND ----------

# After already starting your session
from pyspark.sql.functions import count, sum, avg, max, min

df.groupBy("department").agg( count("employee").alias("employee_count"), 
                              avg("salary").alias("avg_salary"),
                              max("salary").alias("max_salary")
                            ).show()
  

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ----> Multiple aggregations with agg( expr() )
# MAGIC
# MAGIC You can use aggregation and expr() function but apply these changes:
# MAGIC
# MAGIC expr() expects a string argument (a SQL expression), but in teh code above the functions count(), avg(), and max() return Column objects, not strings.
# MAGIC
# MAGIC The fix is simple: expr() should wrap the SQL expression as a string, not wrap Column objects.

# COMMAND ----------

# After already starting your session
from pyspark.sql.functions import count, sum, avg, max, min, col, expr

df.groupBy("department").agg( expr("count(employee) as employee_count"), 
                              expr("avg(salary) as avg_salary"),
                              expr("max(salary) as max_salary")
                              
                            ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Advanced aggregations 
# MAGIC https://www.datacamp.com/tutorial/pyspark-groupby
# MAGIC ###### ---> Pivoting
# MAGIC ###### ---> Rollups and cubes
# MAGIC ###### ---> Grouping sets
# MAGIC ###### ---> Custom aggregation functions
# MAGIC ###### ---> PySpark groupBy Performance Optimization Strategies
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ----> Filtering Aggregated Data
# MAGIC
# MAGIC In PySpark, you can filter groups based on aggregate metrics post-grouping using the filter( ) or where( ) methods. 
# MAGIC -   You need to provide a condition in either Python or SQL expressions. 
# MAGIC -   You can filter BEFORE or AFTER the aggregation. 
# MAGIC     -   Filtering before will impact the aggregation by limiting what data gets aggregated and can improve performance. 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ----> PySpark SQL GROUP BY Query using  a temporary view 
# MAGIC
# MAGIC If you are more comfortable writing SQL, use the SQL API to write statements.
# MAGIC
# MAGIC ###### ---> First create a temporary view 
# MAGIC First step is to create a temporary view using the createOrReplaceTempView() method of the DataFrame
# MAGIC Then you can use spark.sql() to write your statement.

# COMMAND ----------

# Create a temporary view using the DataFrame
df.createOrReplaceTempView("employees")

# Write a SQL-like statement
sql_query = """
    SELECT department, AVG(salary) AS avg_salary
    FROM employees
    GROUP BY department
"""

result_df =  spark.sql(sql_query)
result_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ---------------------------------------------------------------
# MAGIC #### selectExpr() 
# MAGIC ###### In PySpark  You can use selectExpr() to perform aggregations in two primary ways: 
# MAGIC
# MAGIC   -     1 Grouped Aggregation (after a groupBy()).  Use groupBy() then use agg() or selectExpr() 
# MAGIC   -     2 Global Aggregation: either on the entire DataFrame 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 1. Grouped Aggregation (after groupBy())  <<This is the STANDARD/COMMON way>>
# MAGIC To aggregate data within specific groups, you 
# MAGIC    - first use groupBy() and then
# MAGIC    - use agg() or selectExpr() on the resulting GroupedData object. 
# MAGIC
# MAGIC Note that selectExpr() is generally called after the groupBy to select the final columns, which often results in the same effect as using agg()
# MAGIC
# MAGIC A common approach to applying aggregation expressions using SQL syntax within the DataFrame API is to use the agg() method on the grouped data:
# MAGIC
# MAGIC ##### This is teh standard way in pyspark to groupe aggregations. THIS PIECE CONTAINS A field("department") and aggregations(sum and avg) but the field is grouped first

# COMMAND ----------


"""from pyspark.sql.functions import col, sum, avg
df.groupBy("department")
  .agg(sum("salary").alias("total_salary"),
       avg("salary").alias("avg_salary")
      )
"""


# COMMAND ----------

# MAGIC %md
# MAGIC ##### using selectExpr() after groupBy()
# MAGIC

# COMMAND ----------

"""
from pyspark.sql import functions as F

# Sample Data
data = [("Sales", 100), ("Sales", 200), ("Marketing", 50)]
df = spark.createDataFrame(data, ["department", "revenue"])

# Aggregation followed by selectExpr
result = df.groupBy("department") \
           .agg(F.sum("revenue").alias("total")) \
           .selectExpr("department", "total", "total * 1.1 as revenue_with_tax")

result.show()

"""

# COMMAND ----------

# MAGIC %md
# MAGIC ##### If you prefer to use the groupBy() INSIDE THE selectExpr(), you would typically register the DataFrame as a temporary view first, and then run a SQL query:
# MAGIC

# COMMAND ----------

# Using selectExpr() with groupBy is less direct than using agg()
# A common pattern is to register a temp view and use spark.sql()
#df.createOrReplaceTempView("products_view")
#spark.sql("SELECT Product, AVG(Price) AS avg_price, SUM(Quantity) AS total_quantity FROM products_view GROUP BY Product").show()


# COMMAND ----------

# MAGIC %md
# MAGIC -------------------------------------------------------
# MAGIC
# MAGIC
# MAGIC ##### 2. Global Aggregation (on the entire DataFrame) 
# MAGIC You can perform aggregations on the entire DataFrame without explicit grouping. This is a shorthand for df.groupBy().agg()

# COMMAND ----------

"""
from pyspark.sql import SparkSession

# Create a SparkSession (if not already created)
spark = SparkSession.builder.appName("selectExprAgg").getOrCreate()

# Sample DataFrame
data = [("Laptop", 1500, 10), 
        ("Mouse", 50, 200),
        ("Laptop", 1200, 5),
        ("Keyboard", 100, 50)]
columns = ["Product", "Price", "Quantity"]
df = spark.createDataFrame(data, columns)
df.show()


# Global aggregation using selectExpr()
df.selectExpr(
    "avg(Price) AS avg_price",
    "sum(Quantity) AS total_quantity",
    "count(Product) AS total_products"
).show()
"""

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### PAY ATTENTION!! This piece of teh above code DOES NOT apply aggregations to a GROUPBY  any DIMENSION/FIELD, IT ONLY DOES AGGREGATIONS
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ------------------------------------------------------------------------------
# MAGIC #### 3.1  .agg(max("column")).first()[0] # max_value
# MAGIC -------------------------------------------------------------------------------
# MAGIC
# MAGIC In PySpark, the sequence agg(...).first()[0] is a common pattern used to extract a single scalar value from an aggregated DataFrame.
# MAGIC
# MAGIC max_salary = raw_emp_df.agg(max("salary")).first()[0] # max_salary

# COMMAND ----------

"""
Breakdown of the Sequence:

    raw_emp_df: Es el nombre del DataFrame que contiene los datos de los empleados.
    .agg(max("salary")):
        agg es la función de agregación.
        max_("salary") calcula el valor máximo de la columna llamada "salary".
        Nota: El resultado de este paso sigue siendo un DataFrame con una sola fila y una sola columna.
    .first()[0]:
        Esta es una "acción" de Spark que toma la primera fila del DataFrame resultante y la devuelve como un objeto tipo Row de Spark
        [0]: Accede al primer elemento de esa fila (el valor numérico del salario máximo). 
        Sin esto, tendrías un objeto Row(max(salary)=5000), pero con el [0] obtienes simplemente el 5000.
"""

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC -----------------------------------------------------------
# MAGIC #### 4  where/filter/having
# MAGIC -----------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ##### In PySpark
# MAGIC ###### where() and filter() 
# MAGIC Both are used to filter rows based on a given condition, similar to the SQL WHERE clause. They are interchangeable and can accept boolean expressions or SQL-style string conditions
# MAGIC
# MAGIC ###### having() 
# MAGIC In PySpark, 'having' IS NOT USED. You have two options instead:
# MAGIC   -     Applying a filter() or where() condition after the groupBy() and aggregation steps have been performed
# MAGIC   -     Using SQL queries directly 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example: Multiple filters

# COMMAND ----------

# MAGIC %md
# MAGIC ##### SQL Query
# MAGIC
# MAGIC ###### What San Francisco neighborhoods in in the zip codes 94102 and 94103
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT City, Neighborhood, Zipcode
# MAGIC FROM dev.spark_db.sf_fire_calls
# MAGIC WHERE City = 'SF' and Zipcode in (94102 , 94103);

# COMMAND ----------

# MAGIC %md
# MAGIC #####Pyspark
# MAGIC
# MAGIC ###### isin(), &

# COMMAND ----------

from pyspark.sql.window    import Window
from pyspark.sql.functions import rank, col, count, sum, expr, desc

result_df = ( raw_fire_df.where( (raw_fire_df["Zipcode"].isin([94102 , 94103])) & (raw_fire_df["City"]=='SF') )
                         .select("City", "Neighborhood", "Zipcode")
            )

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC -----------------------------------------------------
# MAGIC #### 5 regexp_replace()  
# MAGIC -----------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ##### To replace strings in PySpark, 
# MAGIC the most efficient methods use built-in functions like:
# MAGIC
# MAGIC - regexp_replace for pattern matching or 
# MAGIC - the DataFrame.replace method for direct value substitution.
# MAGIC
# MAGIC
# MAGIC ##### 1. regexp_replace (Pattern-based)
# MAGIC Used for complex string manipulation within a column using regular expressions. It replaces every substring that matches a specific pattern
# MAGIC - Syntax: regexp_replace(column, pattern, replacement)
# MAGIC - Key Feature: Supports regex characters like \d (digits), ^ (start), and $ (end).
# MAGIC

# COMMAND ----------

"""
from pyspark.sql.functions import regexp_replace
# Replaces all digits with 'X' in the "phone" column
df.withColumn("masked_phone", regexp_replace("phone", r"\d", "X"))

"""

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. replace (Value-based)
# MAGIC There are two distinct "replace" methods in PySpark:
# MAGIC
# MAGIC Method 	---------------Level ----------	Description
# MAGIC - df.replace() ----	 DataFrame	--- Replaces exact values across the whole DataFrame or a subset of columns.
# MAGIC - F.replace()	------ Column	   ------ Replaces exact substrings within a string column (Introduced in Spark 3.5.0).
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###### DataFrame replace Example:

# COMMAND ----------

"""
# Replaces "Male" with "M" in the "Gender" column
df.replace("Male", "M", subset=["Gender"])

# Using a dictionary for multiple replacements at once
df.replace({"John": "Jonathan", "Jane": "Janet"}, subset=["Name"])

"""

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Column-Level replace Example:

# COMMAND ----------

"""
from pyspark.sql import functions as F
# Replaces the literal substring "lane" with "ln"
df.withColumn("address", F.replace("address", "lane", "ln"))

"""

# COMMAND ----------

# MAGIC %md
# MAGIC Summary of Differences
# MAGIC
# MAGIC -----------------------------------------
# MAGIC https://www.google.com/search?q=regexp_replace+and+replace+pyspark&client=firefox-b-d&hs=1EyU&sca_esv=91aac8a642e70fa5&biw=1680&bih=739&sxsrf=ANbL-n5PaxFdRNdA4xxVEC9Sgxk7KUL7nw%3A1775176464643&ei=EAvPad-AJ4yrur8P7OKj-Qs&oq=regexp_replace+and+replace+pysp&gs_lp=Egxnd3Mtd2l6LXNlcnAiH3JlZ2V4cF9yZXBsYWNlIGFuZCByZXBsYWNlIHB5c3AqAggAMgUQIRigATIFECEYoAFIuqUBUABYjJEBcAJ4AZABAJgB3gGgAZYTqgEGMS4xNy4xuAEDyAEA-AEBmAIVoAL_FcICCxAuGIAEGLEDGIMBwgIIEC4YgAQYsQPCAggQABiABBixA8ICBRAAGIAEwgIaEC4YgAQYsQMYgwEYlwUY3AQY3gQY4ATYAQHCAgQQIxgnwgIMECMYgAQYExgnGIoFwgIKEAAYgAQYQxiKBcICFhAuGIAEGLEDGNEDGEMYgwEYxwEYigXCAgoQIxiABBgnGIoFwgIKEC4YgAQYQxiKBcICDRAAGIAEGLEDGEMYigXCAgoQABiABBgUGIcCwgIIEAAYgAQYywHCAgcQABiABBgKwgIGEAAYDRgewgIIEAAYChgNGB7CAgQQABgewgIIEAAYFhgKGB7CAgYQABgWGB7CAggQABiABBiiBMICBRAAGO8FmAMAugYGCAEQARgUkgcGMi4xNy4yoAezc7IHBjAuMTcuMrgH8RXCBwswLjMuOC43LjEuMsgHjgKACAA&sclient=gws-wiz-serp
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC -----------------------------------------------------------
# MAGIC #### 6  cast(), try_cast()
# MAGIC -----------------------------------------------------------
# MAGIC
# MAGIC In PySpark,
# MAGIC cast() and try_cast() are used to convert a column from one data type to another, but they handle conversion failures differently, especially when ANSI mode is enabled
# MAGIC
# MAGIC Key Differences
# MAGIC Feature 	
# MAGIC
# MAGIC cast()	
# MAGIC - Success: Returns the converted value.
# MAGIC - Failure (Non-ANSI):	Usually returns null.	
# MAGIC - Failure (ANSI Mode):	Throws an error (e.g., SparkArithmeticException).
# MAGIC
# MAGIC try_cast()
# MAGIC - Success:	Returns the converted value.	
# MAGIC - Failure (Non-ANSI):	Returns null.
# MAGIC - Failure (ANSI Mode):	Returns null instead of failing.
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

"""
from pyspark.sql.functions import col

# Standard cast (may fail in ANSI mode if data is invalid)
df.withColumn("age", col("age_str").cast("int"))

# Try cast (returns NULL if conversion is impossible)
# Available in PySpark 4.0+
df.withColumn("age", col("age_str").try_cast("int"))

df.selectExpr("try_cast(age_str AS int) as age")


"""

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ![image_1774641851028.png](./image_1774641851028.png "image_1774641851028.png")
# MAGIC
# MAGIC -----------------------------------------------------------
# MAGIC ####     7  Dates(datediff, interval)
# MAGIC
# MAGIC - Folder: CH06-Working with Data Types
# MAGIC - Notebook: 04-Working with dates
# MAGIC
# MAGIC ######  Convert String to date        
# MAGIC ######  Add, Subtract days and months to date
# MAGIC ######  Current date, date difference, and interval
# MAGIC ######  Format date 
# MAGIC ######  casting to date fails
# MAGIC
# MAGIC -----------------------------------------------------------
# MAGIC
# MAGIC ##### 1 string field containing date values
# MAGIC A string field containing date values often cannot be cast directly in PySpark using a simple.cast("date") because PySpark expects a default, ISO-compliant date format of yyyy-MM-dd. 
# MAGIC
# MAGIC If your input string is in any other format (e.g., MM/dd/yyyy, dd-MM-yyyy, or yyyyMMdd), the direct cast will likely result in null values or a SparkDateTimeException error.
# MAGIC
# MAGIC #####2 Carefull with columns cotaining STRING 'null'/'Null' values
# MAGIC Columns containing a string 'null'/'Null' values  INSTEAD of containing actual nulls or no values can cause SEVERAL ERRORS
# MAGIC
# MAGIC #####3 expr('try_to_date') 
# MAGIC try_to_date' is a string function, so you do not need to import it, that tries to cast into date, if it fails to do so it will return a Null value.
# MAGIC
# MAGIC #####4 date_format 
# MAGIC when date_format is used it will return  a STRING not a date.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774890256343.png](./image_1774890256343.png "image_1774890256343.png")
# MAGIC
# MAGIC ![image_1774890573091.png](./image_1774890573091.png "image_1774890573091.png")

# COMMAND ----------

# MAGIC %md
# MAGIC #### datediff()
# MAGIC
# MAGIC The pyspark.sql.functions import datediff() function in PySpark calculates the number of days between two dates by evaluating end_date - start_date
# MAGIC
# MAGIC Parameters:
# MAGIC 1)      end: The end date column (Minuend).
# MAGIC 2)      start: The start date column (Subtrahend).
# MAGIC
# MAGIC 3)      If end is after start, the result is positive.
# MAGIC 4)      If end is before start, the result is negative.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### TILL HERE