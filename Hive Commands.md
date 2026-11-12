
# Apache Hive – Command Shortnotes

## 1. Enter Hive

### If Hive is installed directly

```
hive
```

### Using Beeline

```
beeline -u jdbc:hive2://localhost:10000/default
```

Connect with username/password:

```
beeline -u jdbc:hive2://localhost:10000/default -n hive -p password
```

Exit:

```
exit;
```

> **Remember:** Hive CLI is the older interface. Beeline connects to HiveServer2 using JDBC. The exact connection URL depends on your environment.

---

## 2. Basic Hive commands

|Command|Purpose|
|---|---|
|`SHOW DATABASES;`|List databases|
|`SHOW TABLES;`|List tables in current database|
|`SHOW FUNCTIONS;`|List available functions|
|`SHOW PARTITIONS table_name;`|List table partitions|
|`DESCRIBE table_name;`|Display columns and data types|
|`DESCRIBE FORMATTED table_name;`|Detailed table information|
|`DESCRIBE EXTENDED table_name;`|Detailed metadata|
|`SET;`|Display configuration properties|
|`SET property=value;`|Set a configuration property|
|`SELECT VERSION();`|Check Hive version, if supported|

---

## 3. Database management

### Create database

```
CREATE DATABASE company;
```

With a comment:

```
CREATE DATABASE company
COMMENT 'Company database';
```

### Create only if it does not exist

```
CREATE DATABASE IF NOT EXISTS company;
```

### List databases

```
SHOW DATABASES;
```

### Select database

```
USE company;
```

### Describe database

```
DESCRIBE DATABASE company;
```

Detailed information:

```
DESCRIBE DATABASE EXTENDED company;
```

### Drop database

```
DROP DATABASE company;
```

Delete database and its tables:

```
DROP DATABASE company CASCADE;
```

> `DROP DATABASE` normally requires the database to be empty. `CASCADE` removes its tables as well.

---

## 4. Table creation

### Create a managed table

```
CREATE TABLE employees (
    emp_id INT,
    name STRING,
    age INT,
    salary DOUBLE,
    department STRING
);
```

### Create if not exists

```
CREATE TABLE IF NOT EXISTS employees (
    emp_id INT,
    name STRING,
    salary DOUBLE
);
```

### Create table with comments

```
CREATE TABLE employees (
    emp_id INT COMMENT 'Employee ID',
    name STRING COMMENT 'Employee name',
    salary DOUBLE
)
COMMENT 'Employee details';
```

### Create table with row delimiter

```
CREATE TABLE employees (
    emp_id INT,
    name STRING,
    salary DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
```

For tab-separated data:

```
FIELDS TERMINATED BY '\t'
```

For pipe-separated data:

```
FIELDS TERMINATED BY '|'
```

### Create an external table

```
CREATE EXTERNAL TABLE employees_ext (
    emp_id INT,
    name STRING,
    salary DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/employees';
```

**Managed vs External**

|Managed table|External table|
|---|---|
|Hive manages the table data.|Data is stored at an externally specified location.|
|Dropping the table normally deletes its data.|Dropping the table normally preserves the underlying data.|
|Uses the managed warehouse location by default.|Can point to an existing storage location.|

---

## 5. Display and inspect tables

```
SHOW TABLES;
```

Tables matching a pattern:

```
SHOW TABLES LIKE 'emp*';
```

Describe columns:

```
DESCRIBE employees;
```

Detailed metadata:

```
DESCRIBE FORMATTED employees;
```

Display table creation statement:

```
SHOW CREATE TABLE employees;
```

---

## 6. Alter table

### Rename table

```
ALTER TABLE employees RENAME TO staff;
```

### Add columns

```
ALTER TABLE employees
ADD COLUMNS (city STRING, phone STRING);
```

### Change column name and type

```
ALTER TABLE employees
CHANGE name emp_name STRING;
```

### Change column data type

```
ALTER TABLE employees
CHANGE salary salary DOUBLE;
```

### Replace all columns

```
ALTER TABLE employees
REPLACE COLUMNS (
    emp_id INT,
    name STRING,
    department STRING
);
```

### Change table properties

```
ALTER TABLE employees
SET TBLPROPERTIES ('description'='Employee master table');
```

### Drop table

```
DROP TABLE employees;
```

### Drop only if it exists

```
DROP TABLE IF EXISTS employees;
```

### Truncate table

```
TRUNCATE TABLE employees;
```

> `TRUNCATE` removes table data while retaining the table structure. Support and restrictions depend on the table type and Hive version.

---

## 7. Load data into Hive

### Load a local file

```
LOAD DATA LOCAL INPATH '/home/user/employees.csv'
INTO TABLE employees;
```

### Load from HDFS

```
LOAD DATA INPATH '/user/hadoop/employees.csv'
INTO TABLE employees;
```

### Overwrite existing table data

```
LOAD DATA LOCAL INPATH '/home/user/employees.csv'
OVERWRITE INTO TABLE employees;
```

### Load into a partition

```
LOAD DATA LOCAL INPATH '/home/user/it.csv'
INTO TABLE employees
PARTITION (department='IT');
```

**LOCAL vs non-LOCAL**

- `LOCAL`: Source file is on the local filesystem of the machine running the Hive operation.
    
- Without `LOCAL`: Source file is on the Hadoop-compatible filesystem, commonly HDFS.
    
- `OVERWRITE`: Replaces existing data in the target table or partition.
    

---

## 8. Insert data

### Insert one or more rows

```
INSERT INTO TABLE employees
VALUES
(101, 'Alice', 25, 50000, 'IT'),
(102, 'Bob', 28, 60000, 'HR');
```

### Insert using SELECT

```
INSERT INTO TABLE employees_backup
SELECT * FROM employees;
```

### Insert with selected columns

```
INSERT INTO TABLE employees_backup
SELECT emp_id, name, salary
FROM employees;
```

### Insert overwrite

```
INSERT OVERWRITE TABLE employees_backup
SELECT * FROM employees;
```

### Multiple inserts

```
FROM employees
INSERT INTO TABLE employees_it
SELECT * WHERE department='IT'
INSERT INTO TABLE employees_hr
SELECT * WHERE department='HR';
```

**Difference:**

- `INSERT INTO`: Adds data to existing data.
    
- `INSERT OVERWRITE`: Replaces existing data in the target.
    

---

## 9. Basic SELECT queries

```
SELECT * FROM employees;
```

Select particular columns:

```
SELECT name, salary FROM employees;
```

Filter:

```
SELECT * FROM employees
WHERE salary > 50000;
```

Sort:

```
SELECT * FROM employees
ORDER BY salary DESC;
```

Limit:

```
SELECT * FROM employees
LIMIT 5;
```

Distinct:

```
SELECT DISTINCT department FROM employees;
```

Between:

```
SELECT * FROM employees
WHERE salary BETWEEN 30000 AND 60000;
```

IN:

```
SELECT * FROM employees
WHERE department IN ('IT', 'HR');
```

LIKE:

```
SELECT * FROM employees
WHERE name LIKE 'A%';
```

IS NULL:

```
SELECT * FROM employees
WHERE salary IS NULL;
```

---

## 10. Aggregation and grouping

```
SELECT COUNT(*) FROM employees;
SELECT SUM(salary) FROM employees;
SELECT AVG(salary) FROM employees;
SELECT MIN(salary) FROM employees;
SELECT MAX(salary) FROM employees;
```

Group by:

```
SELECT department, COUNT(*)
FROM employees
GROUP BY department;
```

Group by with multiple columns:

```
SELECT department, age, COUNT(*)
FROM employees
GROUP BY department, age;
```

Having:

```
SELECT department, AVG(salary)
FROM employees
GROUP BY department
HAVING AVG(salary) > 50000;
```

**WHERE vs HAVING**

- `WHERE`: Filters rows before grouping.
    
- `HAVING`: Filters groups after aggregation.
    

---

## 11. Joins

### Inner join

```
SELECT e.name, d.department_name
FROM employees e
INNER JOIN departments d
ON e.department = d.department_id;
```

### Left outer join

```
SELECT e.name, d.department_name
FROM employees e
LEFT OUTER JOIN departments d
ON e.department = d.department_id;
```

### Right outer join

```
SELECT e.name, d.department_name
FROM employees e
RIGHT OUTER JOIN departments d
ON e.department = d.department_id;
```

### Full outer join

```
SELECT e.name, d.department_name
FROM employees e
FULL OUTER JOIN departments d
ON e.department = d.department_id;
```

### Left semi join

```
SELECT e.*
FROM employees e
LEFT SEMI JOIN departments d
ON e.department = d.department_id;
```

Returns only left-side records that have a match on the right side. It does not return columns from the right table.

### Left anti join

```
SELECT e.*
FROM employees e
LEFT ANTI JOIN departments d
ON e.department = d.department_id;
```

Returns left-side records that do not have a match on the right side, where supported by the Hive version.

### Cross join

```
SELECT *
FROM employees
CROSS JOIN departments;
```

Produces a Cartesian product.

---

## 12. Subqueries and CTE

### Subquery

```
SELECT *
FROM employees
WHERE salary > (
    SELECT AVG(salary) FROM employees
);
```

### Common Table Expression (CTE)

```
WITH high_salary AS (
    SELECT * FROM employees
    WHERE salary > 50000
)
SELECT * FROM high_salary;
```

CTEs are useful for structuring complex queries and avoiding repeated subquery logic.

---

## 13. Sorting and distribution commands

|Command|Purpose|
|---|---|
|`ORDER BY`|Global ordering of the result.|
|`SORT BY`|Sorting within each reducer.|
|`DISTRIBUTE BY`|Distributes rows to reducers based on specified expressions.|
|`CLUSTER BY`|Combines distribution and sorting on the same expression(s).|

Examples:

```
SELECT * FROM employees
ORDER BY salary DESC;
```

```
SELECT * FROM employees
SORT BY salary DESC;
```

```
SELECT * FROM employees
DISTRIBUTE BY department;
```

```
SELECT * FROM employees
CLUSTER BY department;
```

**Important distinctions:**

- `ORDER BY`: Globally sorts the result; commonly uses a single reducer in traditional Hive execution.
    
- `SORT BY`: Sorts data within each reducer, not globally.
    
- `DISTRIBUTE BY`: Controls which reducer receives each row.
    
- `CLUSTER BY`: Equivalent to `DISTRIBUTE BY` plus `SORT BY` on the same columns, in ascending order.
    

---

## 14. Partitioning

### Create a partitioned table

```
CREATE TABLE sales (
    sale_id INT,
    product STRING,
    amount DOUBLE
)
PARTITIONED BY (year INT, month INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
```

### Static partition insert

```
INSERT INTO TABLE sales
PARTITION (year=2026, month=11)
SELECT sale_id, product, amount
FROM sales_source;
```

### Dynamic partition insert

Enable dynamic partitioning:

```
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
```

Create dynamic partitioned data:

```
INSERT INTO TABLE sales
PARTITION (year, month)
SELECT sale_id, product, amount, year, month
FROM sales_source;
```

The dynamic partition columns must appear at the end of the SELECT list, in the same order as the partition definition.

### Show partitions

```
SHOW PARTITIONS sales;
```

### Add partition manually

```
ALTER TABLE sales
ADD PARTITION (year=2026, month=12);
```

### Drop partition

```
ALTER TABLE sales
DROP PARTITION (year=2026, month=12);
```

### Recover partitions from HDFS directories

```
MSCK REPAIR TABLE sales;
```

Useful when partition directories were created directly in the filesystem and need to be registered in Hive's metastore.

---

## 15. Bucketing

### Create a bucketed table

```
CREATE TABLE employee_bucket (
    emp_id INT,
    name STRING,
    department STRING
)
CLUSTERED BY (emp_id)
INTO 4 BUCKETS
STORED AS ORC;
```

### Sort within buckets

```
CREATE TABLE employee_bucket (
    emp_id INT,
    name STRING,
    department STRING
)
CLUSTERED BY (emp_id)
SORTED BY (emp_id ASC)
INTO 4 BUCKETS
STORED AS ORC;
```

Enable bucketed table enforcement when needed:

```
SET hive.enforce.bucketing=true;
```

Insert:

```
INSERT OVERWRITE TABLE employee_bucket
SELECT emp_id, name, department
FROM employees;
```

**Remember:**

- Partitioning divides data into directories based on partition columns.
    
- Bucketing divides data into a fixed number of files based on a hash of bucket columns.
    
- Partitioning is useful for filtering on partition columns.
    
- Bucketing can help with joins and sampling in suitable workloads.
    

---

## 16. Views

Create a view:

```
CREATE VIEW it_employees AS
SELECT * FROM employees
WHERE department='IT';
```

Query a view:

```
SELECT * FROM it_employees;
```

Replace a view:

```
CREATE OR REPLACE VIEW it_employees AS
SELECT name, salary
FROM employees
WHERE department='IT';
```

Drop a view:

```
DROP VIEW it_employees;
```

A normal view stores a query definition, not an independent copy of the underlying data.

---

## 17. File formats

### TextFile

```
CREATE TABLE emp_text (
    id INT,
    name STRING
)
STORED AS TEXTFILE;
```

### SequenceFile

```
CREATE TABLE emp_seq (
    id INT,
    name STRING
)
STORED AS SEQUENCEFILE;
```

### ORC

```
CREATE TABLE emp_orc (
    id INT,
    name STRING
)
STORED AS ORC;
```

### Parquet

```
CREATE TABLE emp_parquet (
    id INT,
    name STRING
)
STORED AS PARQUET;
```

### Avro

```
CREATE TABLE emp_avro
STORED AS AVRO
TBLPROPERTIES ('avro.schema.url'='hdfs:///schemas/employee.avsc');
```

**Common formats:**

|Format|Type|
|---|---|
|TextFile|Text-based|
|SequenceFile|Binary key-value|
|ORC|Columnar|
|Parquet|Columnar|
|Avro|Row-oriented, schema-based|

---

## 18. Functions

### String functions

```
SELECT UPPER(name) FROM employees;
SELECT LOWER(name) FROM employees;
SELECT LENGTH(name) FROM employees;
SELECT TRIM(name) FROM employees;
SELECT SUBSTR(name, 1, 3) FROM employees;
SELECT CONCAT(name, '_employee') FROM employees;
SELECT REPLACE(name, 'A', 'X') FROM employees;
```

### Numeric functions

```
SELECT ROUND(salary, 2) FROM employees;
SELECT CEIL(salary) FROM employees;
SELECT FLOOR(salary) FROM employees;
SELECT ABS(-100);
SELECT SQRT(25);
```

### Date functions

```
SELECT CURRENT_DATE;
SELECT CURRENT_TIMESTAMP;
SELECT YEAR(CURRENT_DATE);
SELECT MONTH(CURRENT_DATE);
SELECT DAY(CURRENT_DATE);
SELECT DATE_ADD(CURRENT_DATE, 10);
SELECT DATE_SUB(CURRENT_DATE, 5);
```

### Conditional functions

```
SELECT IF(salary > 50000, 'High', 'Low')
FROM employees;
```

```
SELECT
    CASE
        WHEN salary >= 70000 THEN 'High'
        WHEN salary >= 40000 THEN 'Medium'
        ELSE 'Low'
    END
FROM employees;
```

### Null handling

```
SELECT COALESCE(NULL, NULL, 'Default');
SELECT NVL(NULL, 'Default');
SELECT ISNULL(NULL);
```

---

## 19. Explode and LATERAL VIEW

### EXPLODE

Converts array elements into separate rows.

```
SELECT EXPLODE(ARRAY('Python', 'Java', 'R'));
```

### LATERAL VIEW with EXPLODE

```
SELECT student_id, subject
FROM student_subjects
LATERAL VIEW EXPLODE(subjects) temp AS subject;
```

### EXPLODE a map

```
SELECT id, key, value
FROM map_table
LATERAL VIEW EXPLODE(details) temp AS key, value;
```

### POSEXPLODE

Returns element position along with the element.

```
SELECT id, position, subject
FROM student_subjects
LATERAL VIEW POSEXPLODE(subjects) temp AS position, subject;
```

**Remember:** `LATERAL VIEW` allows the output of a UDTF such as `EXPLODE` to be used alongside columns from the original row.

---

## 20. Window functions

### ROW_NUMBER

```
SELECT name, department, salary,
ROW_NUMBER() OVER (
    PARTITION BY department
    ORDER BY salary DESC
) AS rn
FROM employees;
```

### RANK

```
SELECT name, salary,
RANK() OVER (ORDER BY salary DESC) AS rnk
FROM employees;
```

### DENSE_RANK

```
SELECT name, salary,
DENSE_RANK() OVER (ORDER BY salary DESC) AS drnk
FROM employees;
```

### LAG

```
SELECT name, salary,
LAG(salary, 1) OVER (ORDER BY emp_id) AS previous_salary
FROM employees;
```

### LEAD

```
SELECT name, salary,
LEAD(salary, 1) OVER (ORDER BY emp_id) AS next_salary
FROM employees;
```

### Running total

```
SELECT emp_id, salary,
SUM(salary) OVER (
    ORDER BY emp_id
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS running_total
FROM employees;
```

---

## 21. Sampling

### Random percentage sample

```
SELECT * FROM employees
TABLESAMPLE (10 PERCENT);
```

### Bucket-based sampling

```
SELECT * FROM employees
TABLESAMPLE (BUCKET 1 OUT OF 4 ON emp_id);
```

Sampling syntax and supported methods can vary by table format and Hive version.

---

## 22. EXPLAIN and statistics

### Explain query execution

```
EXPLAIN
SELECT department, COUNT(*)
FROM employees
GROUP BY department;
```

### Extended explain

```
EXPLAIN EXTENDED
SELECT * FROM employees;
```

### Analyze table

```
ANALYZE TABLE employees COMPUTE STATISTICS;
```

### Analyze columns

```
ANALYZE TABLE employees
COMPUTE STATISTICS FOR COLUMNS;
```

### Show table statistics

```
DESCRIBE FORMATTED employees;
```

Statistics help Hive's optimizer estimate data sizes and choose suitable execution plans.

---

## 23. Useful configuration commands

Check configuration:

```
SET;
```

Get a specific property:

```
SET hive.exec.dynamic.partition;
```

Set a property:

```
SET hive.exec.dynamic.partition=true;
```

Enable nonstrict dynamic partitioning:

```
SET hive.exec.dynamic.partition.mode=nonstrict;
```

Set reducer count:

```
SET mapreduce.job.reduces=4;
```

Set maximum dynamic partitions:

```
SET hive.exec.max.dynamic.partitions=1000;
```

Some configuration properties depend on the execution engine and Hive version.

---

## 24. Common lab workflow

### Create database and table

```
CREATE DATABASE company;
USE company;

CREATE TABLE employees (
    emp_id INT,
    name STRING,
    salary DOUBLE,
    department STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
```

### Load CSV data

```
LOAD DATA LOCAL INPATH '/home/user/employees.csv'
INTO TABLE employees;
```

### Verify data

```
SHOW TABLES;
DESCRIBE employees;
SELECT * FROM employees;
SELECT COUNT(*) FROM employees;
```

### Perform queries

```
SELECT * FROM employees WHERE salary > 50000;

SELECT department, AVG(salary)
FROM employees
GROUP BY department;

SELECT * FROM employees
ORDER BY salary DESC;
```

### Create an ORC copy

```
CREATE TABLE employees_orc
STORED AS ORC
AS
SELECT * FROM employees;
```

### Clean up

```
DROP TABLE employees_orc;
DROP TABLE employees;
```

---

## 25. Quick revision – Important command families

|Task|Commands|
|---|---|
|Database|`CREATE DATABASE`, `SHOW DATABASES`, `USE`, `DROP DATABASE`|
|Table|`CREATE TABLE`, `CREATE EXTERNAL TABLE`, `SHOW TABLES`, `DESCRIBE`, `ALTER TABLE`, `DROP TABLE`|
|Data loading|`LOAD DATA LOCAL INPATH`, `LOAD DATA INPATH`|
|Data insertion|`INSERT INTO`, `INSERT OVERWRITE`|
|Querying|`SELECT`, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`|
|Joins|`INNER`, `LEFT OUTER`, `RIGHT OUTER`, `FULL OUTER`, `LEFT SEMI`, `LEFT ANTI`|
|Partitioning|`PARTITIONED BY`, `ADD PARTITION`, `DROP PARTITION`, `SHOW PARTITIONS`, `MSCK REPAIR TABLE`|
|Bucketing|`CLUSTERED BY`, `SORTED BY`, `INTO BUCKETS`|
|Sorting|`ORDER BY`, `SORT BY`, `DISTRIBUTE BY`, `CLUSTER BY`|
|Views|`CREATE VIEW`, `CREATE OR REPLACE VIEW`, `DROP VIEW`|
|Complex data|`EXPLODE`, `POSEXPLODE`, `LATERAL VIEW`|
|Window functions|`ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`|
|Optimization|`EXPLAIN`, `ANALYZE TABLE`, `SET`|
|Maintenance|`TRUNCATE TABLE`, `MSCK REPAIR TABLE`|

**Lab reminders**

1. Use `LOCAL` when loading a file from the local filesystem of the Hive client.
    
2. Use `DESCRIBE FORMATTED` to inspect table location, storage format, and partition information.
    
3. Use `SHOW PARTITIONS` to verify partition registration.
    
4. Use `MSCK REPAIR TABLE` when partition directories exist but are not registered in the metastore.
    
5. Dynamic partition columns must be placed at the end of the `SELECT` list in partition-column order.
    
6. `ORDER BY` gives global ordering; `SORT BY` only sorts within reducers.
    
7. `DROP TABLE` on a managed table normally deletes its data; dropping an external table normally preserves the external data.
    
8. `INSERT OVERWRITE` replaces target data; `INSERT INTO` appends data.