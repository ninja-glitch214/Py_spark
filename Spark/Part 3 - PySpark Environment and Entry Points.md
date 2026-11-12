
# Part 3 – PySpark Environment and Entry Points

## 1. Understanding your PySpark environment

First, let's understand what is installed in your OVA.

A typical PySpark environment contains these components:

|Component|Purpose|
|---|---|
|Python|Language used to write PySpark applications|
|PySpark|Python API for Apache Spark|
|Apache Spark|Distributed processing engine|
|JVM|Runtime for Spark's core engine|
|Java|Provides the JVM|
|Hadoop libraries/configuration|Allow Spark to integrate with HDFS and Hadoop services|

Remember the relationship:

```
          Your Python Code
                 |
              PySpark
                 |
          Spark Engine (JVM)
                 |
        Executors / Cluster
```

### Check the environment

Open a terminal in your OVA.

Check Python:

```
python --version
```

Depending on the environment, you may need:

```
python3 --version
```

Check Java:

```
java -version
```

Check Spark:

```
spark-submit --version
```

Check whether PySpark is available as a Python package:

```
python -m pip show pyspark
```

Or:

```
pyspark --version
```

Important

These commands are environment checks. A Spark distribution can be installed and configured even if `pip show pyspark` does not report a separately installed package.

## 2. Starting Spark – PySpark Shell

Imagine you want to experiment with Spark without creating a Python file every time.

You can use the PySpark shell.

It is an interactive Python environment with Spark integration.

Open a terminal and execute:

```
pyspark
```


### Where did `sc` come from?

Notice that we didn't create `sc` manually.

When you start the PySpark shell, it normally initializes these objects for you:

- `spark` → SparkSession
    
- `sc` → SparkContext
    

You can verify this:

```
type(spark)
type(sc)
```

Conceptually:

```
              pyspark
                 |
        Initializes Spark
                 |
        -------------------
        |                 |
      spark               sc
  SparkSession       SparkContext
```

This automatic initialization is convenient in interactive mode.

In a normal `.py` application, you generally create the SparkSession yourself.


## 4. Creating a SparkSession manually

When you write a Python application, you generally need to initialize Spark.

Let's create a simple application.

```
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyFirstSparkApp") \
    .master("local[*]") \
    .getOrCreate()

print("Spark application started!")

spark.stop()
```

### Understand the important methods

1. `SparkSession.builder`

Starts the process of configuring a SparkSession.

2. `appName()`

Sets the name of your Spark application.

```
.appName("MyFirstSparkApp")
```

This name can appear in Spark's application interface and logs.

3. `master()`

Specifies where Spark should execute the application.

```
.master("local[*]")
```

Some common values:

|Master|Meaning|
|---|---|
|`local`|Local execution with one thread|
|`local[2]`|Local execution with two threads|
|`local[*]`|Local execution using available logical cores|
|`yarn`|Use Hadoop YARN|
|`spark://host:7077`|Connect to a Standalone cluster|

4. `getOrCreate()`

Gets an existing SparkSession if one is available, otherwise creates a new one.

5. `stop()`

Stops the SparkSession and its associated SparkContext when the application is finished.

### Why use `getOrCreate()`?

Suppose a SparkSession has already been created in your application.

Calling `getOrCreate()` can reuse the existing session instead of creating another independent session.

## 5. Exploring SparkContext and its properties

Remember from Part 2:

- SparkSession is the primary entry point for DataFrame and SQL operations.
    
- SparkContext is the lower-level entry point for RDD and Spark Core operations.
    

You can obtain SparkContext from SparkSession:

```
sc = spark.sparkContext
```

Let's inspect some useful properties.

```
print("Spark Version:", sc.version)

print("Master:", sc.master)

print("Application Name:", sc.appName)

print("Default Parallelism:", sc.defaultParallelism)
```

Understand what each property tells us:

|Property|Meaning|
|---|---|
|`sc.version`|Spark version|
|`sc.master`|Configured master URL|
|`sc.appName`|Application name|
|`sc.defaultParallelism`|Default level of parallelism suggested by the environment|

For Python-related version information, you can also inspect:

```
import sys

print(sys.version)
```

And:

```
print(sc.pythonVer)
```

`sc.pythonVer` reports the Python version associated with the SparkContext's Python environment.

### Other useful SparkSession properties

```
print(spark.version)

print(spark.sparkContext.master)

print(spark.sparkContext.appName)
```

Notice that SparkSession provides access to the SparkContext.

## 6. Running interactive PySpark commands

Let's perform a small exercise using the PySpark shell.

Start it:

```
pyspark
```

### A. Create an RDD

```
numbers = sc.parallelize([10, 20, 30, 40, 50])
```

`parallelize()` creates an RDD from a collection available in the Python program.

### B. Inspect the RDD

```
print(numbers)
```

Check the number of partitions:

```
numbers.getNumPartitions()
```

The actual partition count depends on the Spark configuration and default parallelism.

You can explicitly specify the number of partitions:

```
numbers = sc.parallelize([10, 20, 30, 40, 50], 3)

numbers.getNumPartitions()
```

Expected output:

```
3
```

### C. Perform transformations

Multiply every number by 2:

```
doubled = numbers.map(lambda x: x * 2)
```

Filter numbers greater than 50:

```
filtered = doubled.filter(lambda x: x > 50)
```

### D. Perform actions

```
filtered.collect()
```

Output:

```
[60, 80, 100]
```

Let's understand what happened:

```
Original RDD
[10, 20, 30, 40, 50]
          |
        map()
      x * 2
          |
[20, 40, 60, 80, 100]
          |
       filter()
        x > 50
          |
   [60, 80, 100]
          |
       collect()
          |
    Python Result
```

Notice that `map()` and `filter()` are transformations, while `collect()` is an action.

This is a practical example of the lazy evaluation concept we studied in Part 2.

## 7. Writing and executing a PySpark `.py` application

Interactive shells are useful for experimentation, but real applications are generally written in Python files.

Let's create our first standalone PySpark application.

### Step 1: Create a Python file

Create a file named:

```
first_spark_app.py
```

### Step 2: Write the program

```

from pyspark.sql import SparkSession

# Create SparkSession
spark = (
    SparkSession.builder
    .appName("FirstSparkApplication")
    .master("local[*]")
    .getOrCreate()
)

# Access SparkContext
sc = spark.sparkContext

# Create an RDD
numbers = sc.parallelize([10, 20, 30, 40, 50])

# Transformation: multiply each number by 2
doubled = numbers.map(lambda x: x * 2)

# Transformation: retain numbers greater than 50
filtered = doubled.filter(lambda x: x > 50)

# Action: execute and retrieve results
result = filtered.collect()

print("Final Result:", result)

# Stop Spark
spark.stop()
```

### Step 3: Execute the application

Open the terminal in the directory containing the file.

Run:

```
spark-submit first_spark_app.py
```

Expected application output:

```
Final Result: [60, 80, 100]
```

You may also see Spark startup logs, warnings and application information in the terminal. Those are not necessarily errors.

### What is happening internally?

```
       Terminal
          |
 spark-submit first_spark_app.py
          |
    Spark Application
          |
        Driver
          |
    SparkContext
          |
    Create RDD
          |
   map() → filter()
          |
       collect()
          |
     Execute Tasks
          |
     Print Result
          |
      spark.stop()
```

The important difference from the interactive shell is that the program is stored in a `.py` file and submitted as an application.

## 8. Understanding `spark-submit`

`spark-submit` is the standard command-line tool used to submit Spark applications.

It allows you to specify how and where the application should run.

Basic syntax:

```
spark-submit [options] application.py [application arguments]
```

Let's examine the important options.

### A. `--master`

Specifies the execution environment or cluster manager.

```
spark-submit --master local[*] first_spark_app.py
```

Or:

```
spark-submit --master yarn first_spark_app.py
```

### B. `--deploy-mode`

Specifies where the Driver should run.

```
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    first_spark_app.py
```

Or:

```
spark-submit \
    --master yarn \
    --deploy-mode client \
    first_spark_app.py
```

Remember:

- Client → Driver on the submitting machine.
    
- Cluster → Driver inside the cluster.
    

### C. `--name`

Sets the application name.

```
spark-submit \
    --name SalesAnalysis \
    first_spark_app.py
```

### D. `--conf`

Specifies Spark configuration properties.

Example:

```
spark-submit \
    --conf spark.executor.memory=2g \
    first_spark_app.py
```

This requests 2 GB of memory for each Executor, subject to cluster-manager allocation and configuration.

### E. `--executor-memory`

Specifies the memory allocated to each Executor.

```
spark-submit \
    --executor-memory 2g \
    first_spark_app.py
```

### F. `--executor-cores`

Specifies the number of CPU cores requested per Executor where supported.

```
spark-submit \
    --executor-cores 2 \
    first_spark_app.py
```

### G. `--num-executors`

Requests the number of Executors where supported by the cluster manager.

```
spark-submit \
    --num-executors 3 \
    first_spark_app.py
```

### Quick revision of options

|Option|Purpose|
|---|---|
|`--master`|Cluster manager / execution target|
|`--deploy-mode`|Driver deployment location|
|`--name`|Application name|
|`--conf`|Set Spark configuration|
|`--executor-memory`|Executor memory|
|`--executor-cores`|Executor CPU cores|
|`--num-executors`|Number of Executors|

Important: Not every option has the same effect in every deployment mode or cluster manager. For example, `--num-executors` is commonly used with YARN, while resource allocation in Standalone and Kubernetes follows their own configuration mechanisms.

Also, you do not need to use every option when running a simple local application.

## 9. Passing command-line arguments to a PySpark application

Suppose you want to execute the same Spark program with different input values.

Instead of editing the Python file every time, you can pass arguments from the terminal.

Python provides the `sys.argv` list for this.

### A. Create a Python application

```

import sys

from pyspark.sql import SparkSession

# Read command-line arguments
if len(sys.argv) < 2:
    print("Usage: spark-submit app.py <name>")
    sys.exit(1)

name = sys.argv[1]

spark = (
    SparkSession.builder
    .appName("ArgumentExample")
    .master("local[*]")
    .getOrCreate()
)

print("Hello", name)

spark.stop()
```

### B. Execute the program

```
spark-submit app.py Rahul
```

Output:

```
Hello Rahul
```

### C. Understand `sys.argv`

Suppose you execute:

```
spark-submit app.py Rahul 25
```

Then conceptually:

```
sys.argv[0]  # app.py
sys.argv[1]  # Rahul
sys.argv[2]  # 25
```

Remember:

- `sys.argv` is a list.
    
- The first element is the script name.
    
- The remaining elements are the arguments.
    
- Arguments are received as strings.
    

If you need a number:

```
age = int(sys.argv[2])
```

Exam trap: `sys.argv[1]` is the first argument supplied by the user, not the script filename.

## 10. Python `lambda`, `map()` and `filter()` in PySpark

Before moving to RDDs in Part 4, let's understand three Python concepts that you will frequently encounter in PySpark.

### A. Lambda – Anonymous function

A lambda is a small function without a conventional function name.

Normal function:

```
def square(x):
    return x * x
```

Equivalent lambda:

```
square = lambda x: x * x
```

Usage:

```
print(square(5))
```

Output:

```
25
```

Syntax:

```
lambda arguments: expression
```

A lambda can contain only a single expression, whose result is returned automatically.

### B. `map()` – Apply a function to every element

Suppose you have:

```
numbers = [1, 2, 3, 4, 5]
```

Using a normal function:

```
def double(x):
    return x * 2

result = list(map(double, numbers))
```

Using lambda:

```
result = list(map(lambda x: x * 2, numbers))
```

Output:

```
[2, 4, 6, 8, 10]
```

In PySpark:

```
numbers = sc.parallelize([1, 2, 3, 4, 5])

result = numbers.map(lambda x: x * 2)

print(result.collect())
```

Output:

```
[2, 4, 6, 8, 10]
```

The important distinction:

- Python `map()` operates on a Python iterable.
    
- PySpark RDD `map()` is a distributed transformation that applies the function to elements of an RDD.
    

### C. `filter()` – Retain elements satisfying a condition

Python:

```
numbers = [1, 2, 3, 4, 5, 6]

result = list(filter(lambda x: x % 2 == 0, numbers))

print(result)
```

Output:

```
[2, 4, 6]
```

PySpark:

```
numbers = sc.parallelize([1, 2, 3, 4, 5, 6])

result = numbers.filter(lambda x: x % 2 == 0)

print(result.collect())
```

Output:

```
[2, 4, 6]
```

### D. Combine `map()` and `filter()`

Suppose we want to:

1. Take numbers from 1 to 10.
    
2. Select only even numbers.
    
3. Square those numbers.
    

```
numbers = sc.parallelize(range(1, 11))

even_numbers = numbers.filter(lambda x: x % 2 == 0)

squared = even_numbers.map(lambda x: x ** 2)

print(squared.collect())
```

Output:

```
[4, 16, 36, 64, 100]
```

Execution flow:

```
       1,2,3,4,5,6,7,8,9,10
                  |
                filter
             Even numbers
                  |
             2,4,6,8,10
                  |
                 map
               Square
                  |
          4,16,36,64,100
                  |
               collect
```

## 11. Practical environment checklist

Since your Spark OVA is already configured, use this checklist to verify your setup before starting Part 4.

PySpark readiness checklist

0/10 completed

Open the Spark OVA and log in.

Open a terminal.

Check Python using python --version or python3 --version.

Check Java using java -version.

Check Spark using spark-submit --version.

Launch the PySpark shell using pyspark.

Verify spark and sc are initialized.

Create an RDD using sc.parallelize().

Run map(), filter() and collect().

Exit the shell and run a standalone .py application using spark-submit.

Reset checklist

### If `pyspark` or `spark-submit` is not recognized

It may be a PATH/configuration issue rather than a missing Spark installation.

Check:

```
which pyspark
which spark-submit
echo $SPARK_HOME
echo $PATH
```

If the commands cannot be found, inspect the OVA's Spark installation and environment configuration before attempting to reinstall anything.

Other common issues:

|Problem|What to check|
|---|---|
|Java not found|Java installation and `JAVA_HOME`|
|`pyspark` not found|Spark installation and `PATH`|
|Python worker fails|Python version and `PYSPARK_PYTHON`|
|Application cannot connect to cluster|Master URL and cluster availability|
|HDFS file not found|HDFS path and file permissions|

## 12. Part 3 – Rapid revision

|Concept|Key point|
|---|---|
|`pyspark`|Starts interactive Python Spark shell|
|`spark-shell`|Starts interactive Scala Spark shell|
|`spark-submit`|Submits a Spark application|
|SparkSession|Main entry point for DataFrame and SQL APIs|
|SparkContext|Lower-level entry point for RDD/Core APIs|
|`parallelize()`|Creates an RDD from a local collection|
|`getNumPartitions()`|Returns the RDD's partition count|
|`collect()`|Returns all RDD elements to the Driver|
|`lambda`|Anonymous single-expression function|
|`map()`|Transforms every element|
|`filter()`|Retains elements satisfying a condition|
|`sys.argv`|Holds command-line arguments|
|`getOrCreate()`|Reuses an existing session or creates one|
|`stop()`|Stops the Spark session/context|

### Important distinctions to remember

- `pyspark` is an interactive shell; `spark-submit` submits an application.
    
- `spark-shell` is Scala-based; `pyspark` is Python-based.
    
- `spark` refers to SparkSession in the usual PySpark shell.
    
- `sc` refers to SparkContext in the usual PySpark shell.
    
- `local[*]` uses available logical cores on the local machine, not every machine in a cluster.
    
- `collect()` is an action and can cause memory problems if the dataset is very large because it brings all results to the Driver.
    
- `map()` and `filter()` on RDDs are transformations and are lazily evaluated.
    

## 13. Practice challenges

Try these in your OVA. These are deliberately problem statements without solutions so you can build practical confidence.

Challenge 1 – Basic RDD operations

- Create an RDD containing numbers from 1 to 50.
    
- Print its number of partitions.
    
- Count its elements.
    
- Display the first five elements.
    
- Calculate the sum of all elements.
    

Challenge 2 – Lambda and filtering

- Create an RDD containing numbers from 1 to 100.
    
- Filter numbers divisible by both 3 and 5.
    
- Square the remaining numbers.
    
- Display the result.
    

Challenge 3 – Standalone application

- Create a Python file that initializes SparkSession.
    
- Accept a number from the command line.
    
- Create an RDD from 1 to that number.
    
- Filter even numbers.
    
- Calculate their sum.
    
- Print the result and stop Spark.
    

Challenge 4 – Spark configuration

Run the same application with:

- `local`
    
- `local[2]`
    
- `local[*]`
    

Inspect the master and partition count. Observe what changes and what does not.

