# Part 5 – DataFrames and Spark SQL

Objective: Learn Spark's primary structured-data interface and how to process structured data using both PySpark commands and SQL.

## 1. Why DataFrames?

RDDs give us low-level control over distributed collections, but working with structured data using RDDs requires us to manually handle many operations.

Consider a sales dataset:

|id|product|quantity|price|
|---|---|---|---|
|1|Laptop|2|50000|
|2|Mouse|5|500|
|3|Keyboard|3|1500|

With an RDD, records may be represented as tuples, and we must manually manage their positions and types.

With a DataFrame, Spark understands the column names, data types and schema. We can write operations such as:

```
df.select("product", "price")
df.filter(df.price > 1000)
df.withColumn("total", df.quantity * df.price)
```

Spark SQL can then optimize structured operations using its query optimizer.

Why DataFrames are preferred for structured data:

- Named columns make data easier to understand and manipulate.
    
- Schema provides column names and data types.
    
- Spark SQL optimizes operations through the Catalyst optimizer.
    
- Supports SQL queries and the PySpark DataFrame API.
    
- Integrates with structured file formats and databases.
    

## 2. RDD vs DataFrame vs Dataset

|Feature|RDD|DataFrame|Dataset|
|---|---|---|---|
|Abstraction|Distributed collection of objects|Distributed table with named columns|Typed distributed collection|
|Schema|No fixed schema|Yes|Yes, with typed objects|
|Optimization|Limited structural optimization|Catalyst optimizer|Catalyst optimizer|
|Type safety|Language-level object types|Column-level schema; not compile-time typed|Compile-time type safety in Scala/Java|
|API|Low-level|Structured API|Structured, typed API|
|PySpark support|Yes|Yes|No typed Dataset API in PySpark|

Important: In Scala and Java, DataFrame is effectively an alias for `Dataset[Row]`. PySpark provides DataFrames but does not expose the typed Dataset API available in Scala/Java.

## 3. Creating DataFrames

There are two common ways to create DataFrames from Python data.

### A. Using `toDF()`

Convert an existing RDD into a DataFrame.

```
data = [
    (1, "Amit", 50000),
    (2, "Ravi", 60000),
    (3, "Neha", 55000)
]

rdd = sc.parallelize(data)

df = rdd.toDF(["id", "name", "salary"])

df.show()
```

Output:

```
+---+----+------+
| id|name|salary|
+---+----+------+
|  1|Amit| 50000|
|  2|Ravi| 60000|
|  3|Neha| 55000|
+---+----+------+
```

- `toDF()` converts an RDD into a DataFrame.
    
- The list supplied gives column names.
    
- Spark infers the data types from the records.
    

You can also use `toDF()` on an existing DataFrame to rename all its columns:

```
df.toDF("emp_id", "emp_name", "salary")
```

This returns a new DataFrame; it does not modify the original.

### B. Using `spark.createDataFrame()`

Create a DataFrame directly from a Python collection, RDD or pandas DataFrame.

```
data = [
    (1, "Amit", 50000),
    (2, "Ravi", 60000),
    (3, "Neha", 55000)
]

df = spark.createDataFrame(
    data,
    ["id", "name", "salary"]
)

df.show()
```

You can also create it from an RDD:

```
rdd = sc.parallelize(data)

df = spark.createDataFrame(
    rdd,
    ["id", "name", "salary"]
)
```

Difference:

|`toDF()`|`createDataFrame()`|
|---|---|
|Called on an RDD (or DataFrame for renaming)|Called on SparkSession|
|Converts an existing RDD|Creates from collections, RDDs and supported inputs|
|Commonly used with RDDs|More general creation method|

## 4. Understanding DataFrame structure

A DataFrame is a distributed collection of rows organized into named columns, with a defined schema.

Example:

```
data = [
    (1, "Amit", 50000.0),
    (2, "Ravi", 60000.0),
    (3, "Neha", 55000.0)
]

df = spark.createDataFrame(
    data,
    ["id", "name", "salary"]
)
```

Conceptual structure:

```
DataFrame
   |
   +-- Schema
   |     +-- id      : Integer
   |     +-- name    : String
   |     +-- salary  : Double
   |
   +-- Rows
         +-- (1, Amit, 50000.0)
         +-- (2, Ravi, 60000.0)
         +-- (3, Neha, 55000.0)
```

- Row: Represents one record.
    
- Column: Represents one named attribute.
    
- Schema: Defines column names, data types and nullability.
    
- DataFrame: The complete distributed table.
    

A DataFrame is immutable. Operations such as adding or dropping columns return a new DataFrame.

## 5. Reading CSV files

Use `spark.read.csv()` or the general `spark.read.format()` interface.

### Basic CSV reading

```
df = spark.read.csv("employees.csv")

df.show()
```

By default, Spark treats the first line as data, not as column headers. It also does not automatically infer numeric types unless requested.

For a CSV file:

```
id,name,salary
1,Amit,50000
2,Ravi,60000
3,Neha,55000
```

The basic read may produce generic column names such as `_c0`, `_c1`, `_c2`, with values represented as strings.

### Reading with options

```
df = spark.read.csv(
    "employees.csv",
    header=True,
    inferSchema=True
)

df.show()
df.printSchema()
```

|Option|Purpose|
|---|---|
|`header=True`|Uses the first row as column names|
|`inferSchema=True`|Attempts to infer column data types|
|`delimiter=";"`|Specifies a custom field separator|
|`schema=...`|Supplies an explicit schema|

These options can also be supplied using `.option()`:

```
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("employees.csv")
)
```

### Reading a semicolon-separated file

```
df = spark.read.csv(
    "employees.csv",
    header=True,
    inferSchema=True,
    sep=";"
)
```

`sep` is an alias for the CSV separator option. You can also use `delimiter` as the option name:

```
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ";")
    .csv("employees.csv")
)
```

### Reading from HDFS

```
df = spark.read.csv(
    "hdfs:///user/company/employees.csv",
    header=True,
    inferSchema=True
)
```

### General format-based reading

```
df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("employees.csv")
)
```

This pattern is useful because `.format()` can be changed to formats such as `"json"`, `"parquet"` or `"orc"`.

Important exam points:

- `header=True` does not infer data types.
    
- `inferSchema=True` attempts to detect types; it may require an additional pass over the data.
    
- Without schema inference or an explicit schema, CSV columns are generally read as strings.
    
- `spark.read.csv()` returns a DataFrame; it does not load the entire dataset into the Python Driver.
    

## 6. Explicit schemas

Instead of asking Spark to infer column types, we can define the schema ourselves.

This is useful when:

- Data types must be predictable.
    
- The input dataset is large and schema inference overhead should be avoided.
    
- You need to control nullability and column definitions.
    
- Input data may contain values that could be interpreted incorrectly.
    

### A. `StructType`

`StructType` represents the complete schema of a DataFrame.

### B. `StructField`

`StructField` represents one column within that schema.

Its general structure is:

```
StructField(
    "column_name",
    data_type,
    nullable=True
)
```

- `column_name`: Column name.
    
- `data_type`: Expected data type.
    
- `nullable`: Whether null values are allowed according to the schema.
    

### C. Important data types

Import the required types:

```
from pyspark.sql.types import *
```

|Data type|Purpose|Example|
|---|---|---|
|`StringType()`|Text|`"Amit"`|
|`IntegerType()`|Integer values|`25`|
|`LongType()`|Larger integer values|`9876543210`|
|`FloatType()`|Single-precision floating point|`12.5`|
|`DoubleType()`|Double-precision floating point|`50000.75`|
|`BooleanType()`|True/False|`True`|
|`DateType()`|Calendar date|`2026-11-12`|
|`TimestampType()`|Date and time|`2026-11-12 10:30:00`|

### D. Defining an explicit schema

```
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType
)

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("salary", DoubleType(), True)
])
```

Here:

- `id` is an integer and is declared non-nullable.
    
- `name` is a string and nullable.
    
- `salary` is a double and nullable.
    

Use this schema while reading the CSV:

```
df = spark.read.csv(
    "employees.csv",
    header=True,
    schema=schema
)

df.printSchema()
df.show()
```

You do not need `inferSchema=True` when supplying an explicit schema.

Important: Declaring a field as non-nullable does not guarantee that every CSV input value will be rejected if it is missing or invalid. Actual parsing behavior depends on the data source and read mode. Validate input data when strict data-quality enforcement is required.

### E. Schema inference vs explicit schema

|Schema inference|Explicit schema|
|---|---|
|Spark determines data types|Developer defines data types|
|Convenient for exploration|Better control and predictability|
|May require extra data scanning|Avoids inference pass|
|Type inference can be inconsistent with dirty data|Incorrect schema can cause parsing/casting issues|

## 7. DataFrame inspection commands

These commands help you understand a DataFrame before processing it.

Assume:

```
df = spark.read.csv(
    "employees.csv",
    header=True,
    inferSchema=True
)
```

### A. `printSchema()`

Displays the schema in a tree-like format.

```
df.printSchema()
```

Example output:

```
root
 |-- id: integer (nullable = true)
 |-- name: string (nullable = true)
 |-- salary: double (nullable = true)
```

Useful for checking data types and nullability.

### B. `schema`

Returns the schema as a `StructType` object.

```
print(df.schema)
```

You can access individual fields:

```
print(df.schema.fields)
print(df.schema["name"].dataType)
```

### C. `dtypes`

Returns column names with their data types as a list of tuples.

```
print(df.dtypes)
```

Example:

```
[
    ("id", "int"),
    ("name", "string"),
    ("salary", "double")
]
```

### D. `columns`

Returns all column names as a Python list.

```
print(df.columns)
```

Output:

```
["id", "name", "salary"]
```

You can check whether a column exists:

```
if "salary" in df.columns:
    print("Salary column exists")
```

### E. `show()`

Displays rows in tabular format.

```
df.show()
```

General syntax:

```
df.show(n=20, truncate=True, vertical=False)
```

|Parameter|Meaning|
|---|---|
|`n`|Number of rows to display; default is 20|
|`truncate`|Truncates long column values; default is `True`|
|`vertical`|Displays each row vertically instead of horizontally|

Examples:

```
df.show(5)                       # First 5 rows
df.show(10, truncate=False)      # Do not truncate long values
df.show(5, vertical=True)        # Vertical display
```

### F. `count()`

Returns the total number of rows.

```
total = df.count()
print(total)
```

Important: `count()` is an action and triggers execution. `show()` also triggers execution. Properties such as `columns`, `dtypes` and `schema` inspect DataFrame metadata.

## 8. Column operations

For these examples, assume:

```
data = [
    (1, "Amit", 50000.0),
    (2, "Ravi", 60000.0),
    (3, "Neha", 55000.0)
]

df = spark.createDataFrame(
    data,
    ["id", "name", "salary"]
)
```

### A. `select()`

Selects specific columns or expressions.

```
df.select("name", "salary").show()
```

Select a single column:

```
df.select("name").show()
```

Select all columns:

```
df.select("*").show()
```

Use expressions:

```
df.select(
    "name",
    (df.salary * 12).alias("annual_salary")
).show()
```

Using `col()`:

```
from pyspark.sql.functions import col

df.select(
    col("name"),
    col("salary")
).show()
```

`alias()` assigns a name to the resulting column expression.

### B. `drop()`

Removes one or more columns.

```
df2 = df.drop("salary")
```

Remove multiple columns:

```
df2 = df.drop("id", "salary")
```

`drop()` returns a new DataFrame; the original remains unchanged.

### C. `withColumn()`

Adds a new column or replaces an existing column with a new expression.

Add a calculated column:

```
from pyspark.sql.functions import col

df2 = df.withColumn(
    "annual_salary",
    col("salary") * 12
)

df2.show()
```

Replace an existing column:

```
df2 = df.withColumn(
    "salary",
    col("salary") + 5000
)
```

Here, the existing `salary` column is replaced in the returned DataFrame.

Add a constant-value column:

```
from pyspark.sql.functions import lit

df2 = df.withColumn("country", lit("India"))
```

`lit()` converts a literal value into a Spark column expression.

### D. `withColumnRenamed()`

Renames an existing column.

```
df2 = df.withColumnRenamed(
    "name",
    "employee_name"
)
```

Rename multiple columns by chaining:

```
df2 = (
    df
    .withColumnRenamed("id", "emp_id")
    .withColumnRenamed("name", "emp_name")
)
```

If the specified column does not exist, the DataFrame is returned without that rename.

### Quick comparison

|Command|Operation|
|---|---|
|`select()`|Selects columns or expressions|
|`drop()`|Removes columns|
|`withColumn()`|Adds or replaces a column|
|`withColumnRenamed()`|Renames a column|

## 9. Filtering rows

Filtering selects rows that satisfy a condition.

### A. `filter()`

```
df.filter(df.salary > 50000).show()
```

Using `col()`:

```
df.filter(col("salary") > 50000).show()
```

Multiple conditions:

```
df.filter(
    (col("salary") > 50000) &
    (col("name") == "Ravi")
).show()
```

### B. `where()`

`where()` is an alias for `filter()`. Both perform the same operation.

```
df.where(col("salary") > 50000).show()
```

### C. Common filtering conditions

Greater than / less than:

```
df.filter(col("salary") >= 50000).show()
```

Equality:

```
df.filter(col("name") == "Amit").show()
```

Not equal:

```
df.filter(col("name") != "Amit").show()
```

AND condition:

```
df.filter(
    (col("salary") > 40000) &
    (col("salary") < 60000)
).show()
```

OR condition:

```
df.filter(
    (col("name") == "Amit") |
    (col("name") == "Neha")
).show()
```

NOT condition:

```
df.filter(~(col("salary") > 50000)).show()
```

IN condition:

```
df.filter(
    col("name").isin("Amit", "Neha")
).show()
```

BETWEEN condition:

```
df.filter(
    col("salary").between(40000, 60000)
).show()
```

NULL checking:

```
df.filter(col("salary").isNull()).show()

df.filter(col("salary").isNotNull()).show()
```

String matching:

```
df.filter(col("name").startswith("A")).show()

df.filter(col("name").contains("it")).show()

df.filter(col("name").endswith("a")).show()
```

Using a SQL condition string:

```
df.filter("salary > 50000").show()
```

This is also valid:

```
df.where("salary > 50000").show()
```

Important Python trap

Use `&`, `|`, and `~` for combining PySpark column conditions, not Python's `and`, `or`, and `not`.

Correct:

```
df.filter(
    (col("salary") > 40000) &
    (col("salary") < 60000)
)
```

Incorrect:

```
df.filter(
    (col("salary") > 40000) and
    (col("salary") < 60000)
)
```

Also, put parentheses around each condition when using `&` or `|`.

## 10. Handling duplicate records

Spark provides two commonly used methods to remove duplicates.

### A. `distinct()`

Removes duplicate rows based on all columns.

```
data = [
    (1, "Amit"),
    (2, "Ravi"),
    (1, "Amit"),
    (3, "Neha")
]

df = spark.createDataFrame(data, ["id", "name"])

df.distinct().show()
```

The repeated `(1, "Amit")` row appears only once.

### B. `dropDuplicates()`

Removes duplicates based on all columns or only the specified columns.

Remove duplicates based on all columns:

```
df.dropDuplicates().show()
```

This behaves like `distinct()`.

Remove duplicates based on a particular column:

```
df.dropDuplicates(["id"]).show()
```

Here, Spark considers only `id` when identifying duplicates.

Remove duplicates based on multiple columns:

```
df.dropDuplicates(["id", "name"]).show()
```

### `distinct()` vs `dropDuplicates()`

|`distinct()`|`dropDuplicates()`|
|---|---|
|Considers all columns|Can consider all or selected columns|
|No subset argument|Supports a subset of columns|
|Removes duplicate rows|Removes rows duplicated according to selected columns|

Important: When duplicates are identified using a subset, do not assume Spark will retain the first or last record. The retained record is not guaranteed to follow an ordering unless you explicitly establish a deterministic selection strategy.

## 11. Temporary views

A temporary view allows you to query a DataFrame using SQL.

Suppose you have:

```
df = spark.createDataFrame(
    [
        (1, "Amit", 50000),
        (2, "Ravi", 60000),
        (3, "Neha", 55000)
    ],
    ["id", "name", "salary"]
)
```

### A. `createOrReplaceTempView()`

Creates a temporary SQL view from a DataFrame.

```
df.createOrReplaceTempView("employees")
```

Now `employees` can be used in Spark SQL queries.

```
result = spark.sql(
    "SELECT * FROM employees"
)

result.show()
```

Why `OrReplace`?

If a temporary view with the same name already exists in the current SparkSession, it is replaced.

### B. Scope of temporary views

- A local temporary view is available within the SparkSession in which it was created.
    
- It is not a permanent table.
    
- It disappears when the SparkSession ends.
    

### C. Global temporary view

To make a temporary view accessible across SparkSessions within the same Spark application:

```
df.createOrReplaceGlobalTempView("employees")
```

Query it using the `global_temp` database:

```
spark.sql(
    "SELECT * FROM global_temp.employees"
).show()
```

|Local temporary view|Global temporary view|
|---|---|
|`createOrReplaceTempView()`|`createOrReplaceGlobalTempView()`|
|Session-scoped|Application-scoped|
|Query by view name|Query using `global_temp.view_name`|
|Removed when its SparkSession ends|Removed when the Spark application ends|

Exam trap: A temporary view is not the same as a permanent Hive table. Creating a view does not create a separate physical copy of the DataFrame's data.

## 12. Executing SQL using `spark.sql()`

Once a DataFrame has been registered as a temporary view, you can use SQL commands to query it.

```
df.createOrReplaceTempView("employees")
```

### A. Select all records

```
result = spark.sql(
    "SELECT * FROM employees"
)

result.show()
```

### B. Select specific columns

```
spark.sql(
    "SELECT name, salary FROM employees"
).show()
```

### C. Filter records

```
spark.sql(
    "SELECT * FROM employees WHERE salary > 50000"
).show()
```

### D. Sort records

```
spark.sql(
    "SELECT * FROM employees ORDER BY salary DESC"
).show()
```

### E. Aggregate records

```
spark.sql("""
    SELECT COUNT(*) AS total_employees,
           AVG(salary) AS average_salary,
           MAX(salary) AS highest_salary
    FROM employees
""").show()
```

### F. Group records

Suppose the DataFrame also contains a `department` column:

```
spark.sql("""
    SELECT department, AVG(salary) AS avg_salary
    FROM employees
    GROUP BY department
""").show()
```

### G. Create a DataFrame from SQL output

`spark.sql()` returns a DataFrame, not a plain Python list.

```
result = spark.sql("""
    SELECT name, salary
    FROM employees
    WHERE salary > 50000
""")

print(type(result))
result.show()
```

You can apply DataFrame operations to the SQL result:

```
result.filter(
    col("salary") > 55000
).show()
```

## 13. Combining SQL and DataFrame APIs

Spark allows you to switch between SQL queries and DataFrame operations. Both interfaces work with the same Spark execution engine.

For example, start with a DataFrame:

```
df = spark.read.csv(
    "employees.csv",
    header=True,
    inferSchema=True
)
```

Step 1 – Register it as a temporary view

```
df.createOrReplaceTempView("employees")
```

Step 2 – Filter using SQL

```
filtered_df = spark.sql("""
    SELECT *
    FROM employees
    WHERE salary > 50000
""")
```

Step 3 – Continue processing using DataFrame API

```
from pyspark.sql.functions import col

result = filtered_df.select(
    "name",
    "salary"
).withColumn(
    "annual_salary",
    col("salary") * 12
)
```

Step 4 – Display the result

```
result.show()
```

Combined processing flow

CSV file → DataFrame

Temporary View

SQL query – Filter records

DataFrame API – Select and calculate columns

Final DataFrame

### SQL vs DataFrame API: same operation

|Operation|DataFrame API|Spark SQL|
|---|---|---|
|Select columns|`df.select("name")`|`SELECT name FROM employees`|
|Filter|`df.filter(col("salary") > 50000)`|`WHERE salary > 50000`|
|Sort|`df.orderBy(col("salary").desc())`|`ORDER BY salary DESC`|
|Remove duplicates|`df.distinct()`|`SELECT DISTINCT *`|
|Add calculated column|`df.withColumn("annual", col("salary") * 12)`|`SELECT *, salary * 12 AS annual`|

Key point: SQL and DataFrame API are two interfaces for structured processing in Spark. You can use whichever is clearer for a particular operation and combine them in the same application.

## 14. Quick revision – Important exam points

|Concept|Remember|
|---|---|
|DataFrame|Distributed data organized into named columns|
|`toDF()`|Converts an RDD to a DataFrame; can also rename DataFrame columns|
|`createDataFrame()`|Creates DataFrame from supported collections, RDDs, etc.|
|`header=True`|First CSV row contains column names|
|`inferSchema=True`|Spark attempts to infer data types|
|`StructType`|Defines the complete schema|
|`StructField`|Defines an individual column|
|`printSchema()`|Displays schema in tree format|
|`dtypes`|Returns column names and type names|
|`columns`|Returns column names|
|`show()`|Displays records|
|`count()`|Returns row count; action|
|`select()`|Selects columns/expressions|
|`withColumn()`|Adds or replaces a column|
|`drop()`|Removes columns|
|`filter()` / `where()`|Filters rows; equivalent|
|`distinct()`|Removes duplicate complete rows|
|`dropDuplicates()`|Supports duplicate removal using selected columns|
|`createOrReplaceTempView()`|Creates/replaces a session-scoped SQL view|
|`spark.sql()`|Executes SQL and returns a DataFrame|

## 15. Practical exercises

Use an `employees.csv` dataset with columns:

`emp_id, name, department, salary, joining_date`

Perform these tasks without referring to solutions:

1. Read the CSV using `header` and `inferSchema`.
    
2. Read the same file using an explicit `StructType` schema.
    
3. Display the schema, column names, data types and first 10 records.
    
4. Find the total number of employees.
    
5. Display only employee names and salaries.
    
6. Add an `annual_salary` column.
    
7. Increase every employee's salary by 10% using `withColumn()`.
    
8. Rename `emp_id` to `employee_id`.
    
9. Remove the `joining_date` column.
    
10. Find employees whose salary is greater than 50,000.
    
11. Find employees belonging to either IT or HR using multiple conditions.
    
12. Find employees whose names start with `A`.
    
13. Remove duplicate employee records.
    
14. Remove duplicates based only on employee ID.
    
15. Register the DataFrame as a temporary view named `employees`.
    
16. Use SQL to find the average salary department-wise.
    
17. Use SQL to find the highest-paid employee.
    
18. Run a SQL query and then perform additional filtering using the DataFrame API.