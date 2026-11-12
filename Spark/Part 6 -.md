# Part 6 – Conditional Logic, UDFs and Complex Data Types

Objective: Learn to manipulate real-world structured and semi-structured data using conditional expressions, custom functions, JSON, date/time, arrays and window operations.

We will use PySpark's built-in functions wherever possible and introduce Python UDFs when Spark's built-in functions are insufficient.

Import commonly used functions:

```
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
```

## A. Conditional expressions

### 1. `when()` and `otherwise()`

In Spark, `when()` is used to create conditional expressions, similar to `if-else` in Python or `CASE WHEN` in SQL.

Syntax:

```
when(condition, value)
```

- `condition`: Boolean condition to evaluate.
    
- `value`: Value returned when the condition is true.
    

`otherwise()` specifies the value when none of the conditions match.

### Example: Categorize employees by salary

Suppose we have:

```
data = [
    (1, "Amit", 30000),
    (2, "Ravi", 60000),
    (3, "Neha", 90000),
    (4, "John", 45000)
]

df = spark.createDataFrame(
    data,
    ["id", "name", "salary"]
)
```

```
df = df.withColumn(
    "salary_category",
    F.when(F.col("salary") >= 80000, "High")
     .when(F.col("salary") >= 50000, "Medium")
     .otherwise("Low")
)

df.show()
```

Output:

```
+---+----+------+---------------+
| id|name|salary|salary_category|
+---+----+------+---------------+
|  1|Amit| 30000|            Low|
|  2|Ravi| 60000|         Medium|
|  3|Neha| 90000|           High|
|  4|John| 45000|            Low|
+---+----+------+---------------+
```

How chained conditions work:

```
salary >= 80000?
       |
    Yes → High
       |
      No
       ↓
salary >= 50000?
       |
    Yes → Medium
       |
      No
       ↓
      Low
```

Conditions are evaluated in order. The first matching condition determines the result.

### 2. Multiple conditions using `&` and `|`

You can combine conditions inside `when()`.

AND condition:

```
df.withColumn(
    "category",
    F.when(
        (F.col("salary") >= 50000) &
        (F.col("salary") < 80000),
        "Eligible"
    ).otherwise("Not Eligible")
).show()
```

OR condition:

```
df.withColumn(
    "category",
    F.when(
        (F.col("salary") < 40000) |
        (F.col("salary") > 80000),
        "Special"
    ).otherwise("Normal")
).show()
```

### 3. Handling noisy data

Conditional expressions can standardize inconsistent or invalid values.

Example: Normalize department names.

```
data = [
    (1, "IT"),
    (2, "it"),
    (3, "Information Technology"),
    (4, "HR"),
    (5, None)
]

df = spark.createDataFrame(data, ["id", "department"])
```

```
df = df.withColumn(
    "department",
    F.when(
        F.upper(F.trim(F.col("department"))).isin(
            "IT", "INFORMATION TECHNOLOGY"
        ),
        "IT"
    )
    .when(
        F.upper(F.trim(F.col("department"))) == "HR",
        "HR"
    )
    .otherwise("Unknown")
)

df.show()
```

This handles leading/trailing spaces, differences in letter case and missing/unrecognized department values.

Important: `when()` without a matching `otherwise()` produces `NULL` for unmatched rows.

## B. User-Defined Functions (UDFs)

### 1. What is a UDF?

A User-Defined Function (UDF) is a custom function that allows us to apply logic to DataFrame columns when the required operation is not conveniently available through Spark's built-in functions.

Example: Suppose we want to convert a numeric score into a custom grade.

```
Score → Custom Python function → Grade
```

Spark already provides many built-in functions. A UDF is useful when the required business logic cannot be expressed conveniently using those functions.

### 2. Defining a Python function for Spark

A normal Python function cannot automatically be used as a DataFrame column expression. It must be registered or wrapped as a Spark UDF.

First, define the Python function:

```
def calculate_grade(score):
    if score >= 90:
        return "A"
    elif score >= 75:
        return "B"
    elif score >= 50:
        return "C"
    else:
        return "F"
```

### 3. Registering a UDF using `udf()`

Import `udf` and specify the return type.

```
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

grade_udf = udf(calculate_grade, StringType())
```

Apply it to a DataFrame column:

```
data = [
    ("Amit", 95),
    ("Ravi", 82),
    ("Neha", 65),
    ("John", 35)
]

df = spark.createDataFrame(data, ["name", "score"])

df = df.withColumn(
    "grade",
    grade_udf(F.col("score"))
)

df.show()
```

Output:

```
+----+-----+-----+
|name|score|grade|
+----+-----+-----+
|Amit|   95|    A|
|Ravi|   82|    B|
|Neha|   65|    C|
|John|   35|    F|
+----+-----+-----+
```

### 4. UDF return types

A Spark UDF requires a return type so Spark knows the output column's data type.

Common return types:

|PySpark type|Python value|
|---|---|
|`StringType()`|`"Hello"`|
|`IntegerType()`|`25`|
|`LongType()`|`9876543210`|
|`FloatType()`|`12.5`|
|`DoubleType()`|`99.75`|
|`BooleanType()`|`True`|
|`DateType()`|Python `date`|
|`TimestampType()`|Python `datetime`|

Example returning an integer:

```
def square(n):
    return n * n

square_udf = udf(square, IntegerType())

df.withColumn(
    "square",
    square_udf(F.col("score"))
).show()
```

Alternative: UDF decorator

You can also declare a UDF using a decorator:

```
@udf(returnType=StringType())
def upper_name(name):
    return name.upper() if name else None
```

Apply it:

```
df.withColumn(
    "upper_name",
    upper_name(F.col("name"))
).show()
```

### 5. Registering a UDF for SQL

A UDF can be registered with the SparkSession so that it can be called inside SQL queries.

```
spark.udf.register(
    "grade_function",
    calculate_grade,
    StringType()
)
```

Register the DataFrame as a temporary view:

```
df.createOrReplaceTempView("students")
```

Use the UDF in SQL:

```
spark.sql("""
    SELECT name, score,
           grade_function(score) AS grade
    FROM students
""").show()
```

Difference:

- `udf()` creates a UDF object for use in the DataFrame API.
    
- `spark.udf.register()` registers a function under a SQL name so it can be called in SQL.
    

### 6. Serialization and performance overhead

A regular Python UDF generally involves communication between the JVM-based Spark engine and Python worker processes.

```
Spark JVM
   |
   | Send column data
   ↓
Python Worker
   |
   | Execute Python UDF
   ↓
Python result
   |
   ↓
Spark JVM
```

This introduces overhead due to:

- Serialization and deserialization of data.
    
- JVM ↔ Python communication.
    
- Python function execution outside the JVM's native Spark SQL execution path.
    

Spark SQL's Catalyst optimizer cannot inspect and optimize the internal logic of a regular Python UDF in the same way it can optimize built-in expressions.

### 7. Built-in functions vs Python UDFs

|Built-in functions|Python UDFs|
|---|---|
|Provided by Spark|Written by developer|
|Executed within Spark's native execution engine|Regular Python UDFs execute in Python workers|
|Better optimization opportunities|Limited optimization of internal function logic|
|Usually faster|Often slower due to communication and serialization overhead|
|Preferred when suitable|Use for custom logic not conveniently supported by built-ins|

Example: Use Spark's built-in `upper()` instead of writing a UDF just to convert text to uppercase.

```
df.withColumn(
    "upper_name",
    F.upper(F.col("name"))
)
```

Exam point: Prefer built-in Spark functions over Python UDFs whenever possible. Pandas UDFs use Apache Arrow for more efficient batch-based data transfer, but they are still not automatically faster for every workload.

### 8. `monotonically_increasing_id()`

Generates a unique, monotonically increasing 64-bit ID for each row within a DataFrame execution.

```
df = df.withColumn(
    "generated_id",
    F.monotonically_increasing_id()
)

df.show()
```

Important characteristics:

- IDs are unique within the generated DataFrame result.
    
- IDs increase monotonically, but are not necessarily consecutive.
    
- The generated values depend on partitioning and row position within partitions.
    
- It does not guarantee IDs starting from 1.
    
- It is not a substitute for a database auto-increment or globally stable business key.
    

Conceptually, Spark encodes partition information and row-position information into the generated ID.

For example, IDs might look like:

```
0
1
2
8589934592
8589934593
```

The gap can occur because records belong to different partitions.

Exam trap: `monotonically_increasing_id()` does not generate consecutive integers.

## C. JSON processing

JSON is commonly used in APIs, event streams, logs and semi-structured datasets. Unlike CSV, JSON can contain nested objects and arrays.

### 1. Reading JSON files

Use:

```
df = spark.read.json("employees.json")
```

Example JSON file:

```
{"id": 1, "name": "Amit", "salary": 50000}
{"id": 2, "name": "Ravi", "salary": 60000}
```

This is newline-delimited JSON (NDJSON), where each line is an independent JSON object.

Spark reads each JSON object as a row and infers a schema by default.

```
df.show()
df.printSchema()
```

### 2. Single-line vs multiline JSON

A. Single-line JSON / JSON Lines

Each line contains one complete JSON object.

```
{"id": 1, "name": "Amit"}
{"id": 2, "name": "Ravi"}
{"id": 3, "name": "Neha"}
```

Read using the default setting:

```
df = spark.read.json("employees.json")
```

B. Multiline JSON

A complete JSON document can span multiple lines.

```
[
  {
    "id": 1,
    "name": "Amit"
  },
  {
    "id": 2,
    "name": "Ravi"
  }
]
```

Read it using:

```
df = spark.read.option(
    "multiLine", "true"
).json("employees.json")
```

|Single-line JSON|Multiline JSON|
|---|---|
|One JSON object per line|A JSON document may span multiple lines|
|Default `multiLine=False`|Requires `multiLine=True`|
|Commonly used for streaming/event records|Commonly used for formatted JSON documents or arrays|

Important: `multiLine=True` changes how Spark reads the JSON document. It does not mean that every JSON object must be formatted across multiple lines.

### 3. Nested JSON structures

Real-world JSON often contains nested objects and arrays.

Example:

```
{
  "id": 1,
  "name": "Amit",
  "address": {
    "city": "Pune",
    "state": "Maharashtra"
  },
  "skills": ["Python", "Spark", "SQL"]
}
```

Read it:

```
df = spark.read.json("employee.json")
df.printSchema()
```

Conceptual schema:

```
root
 |-- id: long
 |-- name: string
 |-- address: struct
 |    |-- city: string
 |    |-- state: string
 |-- skills: array
      |-- element: string
```

Access nested fields using dot notation:

```
df.select(
    "name",
    "address.city",
    "address.state"
).show()
```

Access an array column:

```
df.select("name", "skills").show()
```

Access an individual array element using `getItem()`:

```
df.select(
    "name",
    F.col("skills").getItem(0).alias("first_skill")
).show()
```

Array indexing is zero-based.

### 4. `from_json()`

Converts a JSON-formatted string column into a structured Spark column using a specified schema.

This is especially useful when JSON is stored as a string inside a CSV, Kafka record or another DataFrame.

Example:

```
data = [
    (1, '{"city":"Pune","pin":411001}'),
    (2, '{"city":"Mumbai","pin":400001}')
]

df = spark.createDataFrame(
    data,
    ["id", "address_json"]
)
```

Define the schema:

```
address_schema = StructType([
    StructField("city", StringType(), True),
    StructField("pin", IntegerType(), True)
])
```

Convert the string into a structured column:

```
df = df.withColumn(
    "address",
    F.from_json(
        F.col("address_json"),
        address_schema
    )
)
```

Extract nested fields:

```
df.select(
    "id",
    "address.city",
    "address.pin"
).show()
```

Conceptually:

```
JSON string
    |
    | from_json(schema)
    ↓
Structured column (struct)
    |
    +-- city
    +-- pin
```

Important: `from_json()` parses JSON text into a structured value. It does not simply extract one field as text.

### 5. `to_json()`

Converts a Spark struct, array or map column into a JSON-formatted string.

Using the structured `address` column from the previous example:

```
df = df.withColumn(
    "address_text",
    F.to_json(F.col("address"))
)

df.select("id", "address_text").show(truncate=False)
```

Possible output:

```
+---+--------------------------------+
|id |address_text                    |
+---+--------------------------------+
|1  |{"city":"Pune","pin":411001}    |
|2  |{"city":"Mumbai","pin":400001}  |
+---+--------------------------------+
```

Difference:

|`from_json()`|`to_json()`|
|---|---|
|JSON string → structured column|Struct/array/map → JSON string|
|Parses JSON|Serializes structured data|
|Requires a schema|Converts the supplied complex column|

### 6. `json_tuple()`

Extracts multiple top-level fields from a JSON string column and returns them as separate string columns.

Example:

```
data = [
    (1, '{"name":"Amit","city":"Pune","age":25}'),
    (2, '{"name":"Ravi","city":"Mumbai","age":30}')
]

df = spark.createDataFrame(
    data,
    ["id", "json_data"]
)
```

Use `json_tuple()`:

```
df.select(
    "id",
    F.json_tuple(
        F.col("json_data"),
        "name",
        "city",
        "age"
    ).alias("name", "city", "age")
).show()
```

Output:

```
+---+----+------+---+
| id|name|  city|age|
+---+----+------+---+
|  1|Amit|  Pune| 25|
|  2|Ravi|Mumbai| 30|
+---+----+------+---+
```

Important characteristics:

- Extracts multiple specified fields from a JSON string.
    
- Produces string values.
    
- Intended for top-level fields, not arbitrary nested-path extraction.
    
- For typed nested structures, prefer `from_json()` with an explicit schema.
    

### JSON function comparison

|Function|Purpose|
|---|---|
|`spark.read.json()`|Reads JSON files into a DataFrame|
|`from_json()`|Parses a JSON string into a structured column|
|`to_json()`|Converts a struct/array/map into JSON text|
|`json_tuple()`|Extracts multiple top-level fields as strings|

## D. Date and time functions

Date/time functions are used to extract date components, calculate differences and perform date arithmetic.

Import:

```
from pyspark.sql import functions as F
```

Example dataset:

```
data = [
    (1, "Amit", "2024-01-15"),
    (2, "Ravi", "2023-06-20"),
    (3, "Neha", "2025-03-10")
]

df = spark.createDataFrame(
    data,
    ["id", "name", "joining_date"]
)
```

Convert the date string into a Spark date:

```
df = df.withColumn(
    "joining_date",
    F.to_date("joining_date", "yyyy-MM-dd")
)
```

### 1. `to_date()`

Converts a string or timestamp column into a DateType column.

Syntax:

```
to_date(column, format)
```

Example:

```
df.withColumn(
    "date_value",
    F.to_date("joining_date", "yyyy-MM-dd")
)
```

For a different input format:

```
df.withColumn(
    "date_value",
    F.to_date("date_string", "dd-MM-yyyy")
)
```

If the format is omitted, Spark attempts to parse using its supported date formats.

### 2. `year()`

Extracts the year from a date or timestamp.

```
df.withColumn(
    "joining_year",
    F.year("joining_date")
).show()
```

### 3. `month()`

Extracts the month number from a date or timestamp.

```
df.withColumn(
    "joining_month",
    F.month("joining_date")
).show()
```

Returns an integer from 1 to 12.

### 4. `date_format()`

Formats a date or timestamp as a string using a specified pattern.

```
df.withColumn(
    "formatted_date",
    F.date_format("joining_date", "dd-MM-yyyy")
).show()
```

Example format patterns:

|Pattern|Meaning|Example|
|---|---|---|
|`yyyy`|Four-digit year|2024|
|`yy`|Two-digit year|24|
|`MM`|Month number|01|
|`MMM`|Short month name|Jan|
|`MMMM`|Full month name|January|
|`dd`|Day of month|15|
|`HH`|Hour (24-hour)|14|
|`mm`|Minute|30|
|`ss`|Second|45|

Important: `date_format()` returns a string, not a DateType column.

### 5. `datediff()`

Calculates the difference between two dates in days.

Syntax:

```
datediff(end_date, start_date)
```

Example:

```
df.withColumn(
    "days_since_joining",
    F.datediff(
        F.current_date(),
        F.col("joining_date")
    )
).show()
```

The result is:

```
end_date - start_date
```

If the end date is earlier than the start date, the result is negative.

### 6. `months_between()`

Calculates the number of months between two dates.

Syntax:

```
months_between(date1, date2)
```

Example:

```
df.withColumn(
    "months_worked",
    F.months_between(
        F.current_date(),
        F.col("joining_date")
    )
).show()
```

The result may contain a fractional number of months.

Exam point: If both dates fall on the same day of the month, or both are the last day of their respective months, Spark returns a whole-number month difference.

### 7. `add_months()`

Adds or subtracts a specified number of months from a date.

Syntax:

```
add_months(date, num_months)
```

Example – add 6 months:

```
df.withColumn(
    "after_6_months",
    F.add_months("joining_date", 6)
).show()
```

Subtract 3 months:

```
df.withColumn(
    "before_3_months",
    F.add_months("joining_date", -3)
).show()
```

### Date function revision

|Function|Purpose|Return type|
|---|---|---|
|`to_date()`|Converts to date|Date|
|`year()`|Extracts year|Integer|
|`month()`|Extracts month|Integer|
|`date_format()`|Formats date as text|String|
|`datediff()`|Difference in days|Integer|
|`months_between()`|Difference in months|Double|
|`add_months()`|Adds/subtracts months|Date|

## E. Array functions

An array column stores multiple values within a single row.

Example:

```
data = [
    (1, "Amit", ["Python", "SQL", "Spark"]),
    (2, "Ravi", ["Java", "SQL"]),
    (3, "Neha", ["Python", "R"])
]

df = spark.createDataFrame(
    data,
    ["id", "name", "skills"]
)

df.printSchema()
```

Conceptual schema:

```
root
 |-- id: long
 |-- name: string
 |-- skills: array
      |-- element: string
```

### 1. `explode()`

Converts each element of an array into a separate row.

```
df.select(
    "id",
    "name",
    F.explode("skills").alias("skill")
).show()
```

Output:

```
+---+----+--------+
| id|name|   skill|
+---+----+--------+
|  1|Amit|  Python|
|  1|Amit|     SQL|
|  1|Amit|   Spark|
|  2|Ravi|    Java|
|  2|Ravi|     SQL|
|  3|Neha|  Python|
|  3|Neha|       R|
+---+----+--------+
```

Conceptually:

```
Before explode:

Amit → [Python, SQL, Spark]

After explode:

Amit → Python
Amit → SQL
Amit → Spark
```

Important: `explode()` increases the number of rows. For a null or empty array, regular `explode()` produces no output row for that input record.

### 2. `array_contains()`

Checks whether an array contains a particular value.

Returns `true` or `false` (or `NULL` when the array itself is null).

```
df.filter(
    F.array_contains(
        F.col("skills"),
        "Python"
    )
).show()
```

This selects employees whose skills array contains `"Python"`.

You can also create a Boolean column:

```
df.withColumn(
    "knows_python",
    F.array_contains("skills", "Python")
).show()
```

### 3. `size()`

Returns the number of elements in an array or map.

```
df.withColumn(
    "skill_count",
    F.size("skills")
).show()
```

Example:

```
Amit → 3
Ravi → 2
Neha → 2
```

### 4. `sort_array()`

Sorts array elements in ascending order by default.

```
df.withColumn(
    "sorted_skills",
    F.sort_array("skills")
).show(truncate=False)
```

Descending order:

```
df.withColumn(
    "sorted_skills",
    F.sort_array("skills", asc=False)
).show(truncate=False)
```

For arrays of comparable values, sorting follows their natural ordering.

### 5. `split()`

Splits a string into an array using a regular expression pattern.

Example:

```
data = [
    (1, "Python,SQL,Spark"),
    (2, "Java,SQL"),
    (3, "R,Python")
]

df = spark.createDataFrame(
    data,
    ["id", "skills"]
)
```

```
df = df.withColumn(
    "skills_array",
    F.split("skills", ",")
)

df.show(truncate=False)
```

Output:

```
+---+-----------------+---------------------+
|id |skills           |skills_array         |
+---+-----------------+---------------------+
|1  |Python,SQL,Spark  |[Python, SQL, Spark] |
|2  |Java,SQL          |[Java, SQL]          |
|3  |R,Python           |[R, Python]          |
+---+-----------------+---------------------+
```

Split using whitespace:

```
df.withColumn(
    "words",
    F.split("skills", "\\s+")
)
```

Important: The delimiter is a regular expression, not necessarily a literal string. For example, `.` has a special meaning in regex; use `"\\."` to split on an actual dot.

### Connecting array functions

A common data-cleaning workflow:

```
String column
    |
    | split()
    ↓
Array column
    |
    | array_contains() / size()
    ↓
Filter or inspect array
    |
    | explode()
    ↓
One element per row
    |
    | sort_array()
    ↓
Sorted array (if required)
```

Function comparison:

|Function|Operation|
|---|---|
|`split()`|String → Array|
|`explode()`|Array → Multiple rows|
|`array_contains()`|Checks whether a value exists|
|`size()`|Counts elements|
|`sort_array()`|Sorts array elements|

## F. Window functions

### 1. What are window functions?

Window functions perform calculations across a group of related rows while preserving individual rows.

For example, suppose we have employee salaries:

|Name|Department|Salary|
|---|---|---|
|Amit|IT|60000|
|Ravi|IT|50000|
|Neha|IT|50000|
|John|HR|40000|
|Priya|HR|30000|

Suppose we want to:

- Rank employees within each department.
    
- Find the previous employee's salary.
    
- Find the next employee's salary.
    
- Calculate cumulative salary department-wise.
    

A normal `GROUP BY` combines rows into groups. A window function calculates over related rows but retains each employee's row.

### 2. Window specification

A window specification defines which rows participate in a calculation and in what order.

It is created using `Window`.

```
from pyspark.sql.window import Window
```

General structure:

```
window_spec = (
    Window
    .partitionBy("department")
    .orderBy("salary")
)
```

Two important methods:

A. `Window.partitionBy()`

Divides rows into independent groups for window calculations.

```
Window.partitionBy("department")
```

- IT employees form one window.
    
- HR employees form another window.
    
- Calculations happen independently in each department.
    

Without `partitionBy()`, the entire DataFrame is treated as one window.

B. `Window.orderBy()`

Defines the order of rows within each window.

```
Window.orderBy("salary")
```

Descending order:

```
Window.orderBy(F.col("salary").desc())
```

Combined:

```
window_spec = (
    Window
    .partitionBy("department")
    .orderBy(F.col("salary").desc())
)
```

Conceptual structure:

```
DataFrame
    |
    | partitionBy(department)
    |
    +-- IT window
    |     |
    |     +-- Order by salary DESC
    |
    +-- HR window
          |
          +-- Order by salary DESC
```

### Example dataset

```
data = [
    (1, "Amit", "IT", 60000),
    (2, "Ravi", "IT", 50000),
    (3, "Neha", "IT", 50000),
    (4, "John", "HR", 40000),
    (5, "Priya", "HR", 30000)
]

df = spark.createDataFrame(
    data,
    ["id", "name", "department", "salary"]
)
```

### 3. `row_number()`

Assigns a unique sequential number to every row within a window, starting from 1.

```
window_spec = (
    Window
    .partitionBy("department")
    .orderBy(F.col("salary").desc())
)

df.withColumn(
    "row_num",
    F.row_number().over(window_spec)
).show()
```

Conceptual result:

|Name|Department|Salary|row_num|
|---|---|---|---|
|Amit|IT|60000|1|
|Ravi|IT|50000|2|
|Neha|IT|50000|3|
|John|HR|40000|1|
|Priya|HR|30000|2|

Ravi and Neha have the same salary, but receive different row numbers.

Use case: Selecting the top 2 employees in each department.

```
ranked_df = df.withColumn(
    "row_num",
    F.row_number().over(window_spec)
)

ranked_df.filter(
    F.col("row_num") <= 2
).show()
```

### 4. `rank()`

Assigns the same rank to rows having equal ordering values. It leaves gaps after tied ranks.

```
df.withColumn(
    "rank",
    F.rank().over(window_spec)
).show()
```

Conceptual IT result:

|Name|Salary|rank|
|---|---|---|
|Amit|60000|1|
|Ravi|50000|2|
|Neha|50000|2|

The next rank would be 4 if another lower-paid employee existed.

### 5. `dense_rank()`

Also assigns equal ranks to tied values, but does not leave gaps.

```
df.withColumn(
    "dense_rank",
    F.dense_rank().over(window_spec)
).show()
```

Conceptual result:

|Name|Salary|dense_rank|
|---|---|---|
|Amit|60000|1|
|Ravi|50000|2|
|Neha|50000|2|
|John|40000|3|

### `row_number()` vs `rank()` vs `dense_rank()`

Suppose the ordered salaries are:

```
100, 90, 90, 80
```

|Salary|`row_number()`|`rank()`|`dense_rank()`|
|---|---|---|---|
|100|1|1|1|
|90|2|2|2|
|90|3|2|2|
|80|4|4|3|

Important exam distinction

- `row_number()` → Always unique sequential numbering.
    
- `rank()` → Ties share rank; gaps appear.
    
- `dense_rank()` → Ties share rank; no gaps.
    

### 6. `lag()`

Returns a value from a previous row within the specified window.

Example: Compare each employee's salary with the previous employee's salary in the same department.

```
df.withColumn(
    "previous_salary",
    F.lag("salary", 1).over(window_spec)
).show()
```

General syntax:

```
lag(column, offset=1, default=None)
```

- `column`: Column whose previous value is required.
    
- `offset`: Number of rows to look backward; default is 1.
    
- `default`: Value returned when the requested preceding row does not exist.
    

For the first row in each department, the previous salary is `NULL` by default.

### 7. `lead()`

Returns a value from a following row within the specified window.

```
df.withColumn(
    "next_salary",
    F.lead("salary", 1).over(window_spec)
).show()
```

General syntax:

```
lead(column, offset=1, default=None)
```

The last row in each department has no following row, so the result is `NULL` by default.

### `lead()` vs `lag()`

|`lag()`|`lead()`|
|---|---|
|Looks at previous rows|Looks at following rows|
|Looks backward|Looks forward|
|Useful for previous-period comparison|Useful for next-period comparison|

### 8. Cumulative aggregations

Window functions can calculate running totals, averages, maximums and other aggregations.

Suppose we want a cumulative salary total for each department.

```
window_spec = (
    Window
    .partitionBy("department")
    .orderBy("salary")
    .rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow
    )
)
```

Apply cumulative `sum()`:

```
df.withColumn(
    "cumulative_salary",
    F.sum("salary").over(window_spec)
).show()
```

Understanding `rowsBetween()`:

It defines the row boundaries included in the window frame.

|Boundary|Meaning|
|---|---|
|`Window.unboundedPreceding`|Start from the first row in the window|
|`Window.currentRow`|End at the current row|
|`Window.unboundedFollowing`|Extend to the last row in the window|
|`-1`|One row before the current row|
|`1`|One row after the current row|

For example:

```
.rowsBetween(
    Window.unboundedPreceding,
    Window.currentRow
)
```

Means:

```
Row 1 → Row 1
Row 2 → Rows 1–2
Row 3 → Rows 1–3
Row 4 → Rows 1–4
```

The result is a running calculation.

Other cumulative calculations:

```
# Cumulative average
df.withColumn(
    "running_avg",
    F.avg("salary").over(window_spec)
)

# Cumulative maximum
df.withColumn(
    "running_max",
    F.max("salary").over(window_spec)
)

# Cumulative minimum
df.withColumn(
    "running_min",
    F.min("salary").over(window_spec)
)
```

Important: When using ordered window aggregations, explicitly specifying `rowsBetween()` is useful when you need row-by-row cumulative behavior. The default ordered window frame is generally a range-based frame, which can treat tied ordering values as peers.

### 9. Complete window function example

```
from pyspark.sql import functions as F
from pyspark.sql.window import Window

window_spec = (
    Window
    .partitionBy("department")
    .orderBy(F.col("salary").desc())
)

result = (
    df
    .withColumn(
        "row_number",
        F.row_number().over(window_spec)
    )
    .withColumn(
        "rank",
        F.rank().over(window_spec)
    )
    .withColumn(
        "dense_rank",
        F.dense_rank().over(window_spec)
    )
    .withColumn(
        "previous_salary",
        F.lag("salary").over(window_spec)
    )
    .withColumn(
        "next_salary",
        F.lead("salary").over(window_spec)
    )
)

result.show()
```

This single example demonstrates partitioning, ordering, ranking and accessing neighboring rows.

## Quick revision – Part 6

|Topic|Command|Core purpose|
|---|---|---|
|Conditional|`when()`|Evaluates a condition|
|Conditional|`otherwise()`|Default result|
|UDF|`udf()`|Wraps a Python function for Spark|
|UDF|`spark.udf.register()`|Registers a function for SQL|
|ID generation|`monotonically_increasing_id()`|Generates unique, non-consecutive IDs|
|JSON|`spark.read.json()`|Reads JSON files|
|JSON|`from_json()`|JSON string → structured value|
|JSON|`to_json()`|Structured value → JSON string|
|JSON|`json_tuple()`|Extracts top-level JSON fields|
|Date|`datediff()`|Difference in days|
|Date|`months_between()`|Difference in months|
|Date|`add_months()`|Adds/subtracts months|
|Date|`date_format()`|Formats date as string|
|Date|`to_date()`|Converts to DateType|
|Date|`year()`, `month()`|Extracts date components|
|Array|`explode()`|Array elements → rows|
|Array|`array_contains()`|Checks element membership|
|Array|`size()`|Counts array elements|
|Array|`sort_array()`|Sorts array|
|Array|`split()`|String → array|
|Window|`partitionBy()`|Divides rows into windows|
|Window|`orderBy()`|Defines row order|
|Window|`row_number()`|Unique sequential number|
|Window|`rank()`|Tied ranks with gaps|
|Window|`dense_rank()`|Tied ranks without gaps|
|Window|`lag()`|Previous row's value|
|Window|`lead()`|Following row's value|
|Window|`rowsBetween()`|Defines window frame|

## Practical exercises

Use these exercises to practise the commands without solutions.

A. Conditional expressions and UDFs

1. Categorize employees into High, Medium and Low salary groups.
    
2. Categorize students into grades A, B, C and Fail based on marks.
    
3. Replace null or invalid department names with `"Unknown"`.
    
4. Create a Python UDF that converts a student's marks into a grade.
    
5. Register the UDF for SQL use.
    
6. Generate unique IDs using `monotonically_increasing_id()` and inspect the results.
    

B. JSON processing

7. Read a newline-delimited JSON file.
    
8. Read a multiline JSON file containing an array of objects.
    
9. Read nested JSON containing employee address and skills.
    
10. Extract nested city and state fields.
    
11. Convert a JSON string column into a struct using `from_json()`.
    
12. Convert a struct column into JSON using `to_json()`.
    
13. Extract name, city and age using `json_tuple()`.
    

C. Date and time

14. Convert a date string into DateType.
    
15. Extract year and month from joining dates.
    
16. Calculate employee tenure in days.
    
17. Calculate employee tenure in months.
    
18. Find the date six months after joining.
    
19. Format joining dates as `dd-MM-yyyy`.
    

D. Array operations

20. Convert comma-separated skills into an array using `split()`.
    
21. Find employees who know Python using `array_contains()`.
    
22. Count the number of skills per employee.
    
23. Sort skills alphabetically.
    
24. Convert each skill into a separate row using `explode()`.
    

E. Window functions

25. Assign row numbers to employees within each department.
    
26. Rank employees by salary within each department.
    
27. Compare `rank()` and `dense_rank()` when salaries are tied.
    
28. Find the second-highest salary in each department.
    
29. Find each employee's previous and next salary using `lag()` and `lead()`.
    
30. Calculate cumulative salary department-wise.
    
31. Find the top three employees in each department.