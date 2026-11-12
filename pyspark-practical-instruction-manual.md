# PySpark Practical Instruction Manual & Command Reference Guide

This instruction manual serves as a definitive operational guide for developing and executing big data pipelines using Apache Spark and PySpark. It is structured specifically for hands-on lab environments, virtual machine sandboxes, and production-grade distributed data workflows. Each section details foundational mechanics, programmatic syntax, copy-pasteable execution templates, and critical performance gotchas grounded in the official course practicals.

---

## 1. System Environment & Session Initialization

Apache Spark applications interface with cluster compute resources through client-side driver processes. Configuring execution runtimes, environment variables, and entry points correctly is essential before executing distributed jobs.

### 1.1 Environment Variable Configuration
In virtual machine (VM) and multi-version Python environments, the driver and executor processes must explicitly bind to matching Python interpreters to prevent serialization errors.

```bash
# Set Python runtime bindings for driver and worker executors
export PYSPARK_PYTHON=/usr/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3

# Verify installed PySpark and Java dependencies
java -version
pyspark --version
```

### 1.2 SparkContext (`sc`) — Low-Level Entry Point
The `SparkContext` represents the initial connection to a Spark cluster and is used to create low-level Resilient Distributed Datasets (RDDs). Standalone scripts can initialize it directly using `SparkConf`.

```python
from pyspark import SparkConf, SparkContext

# Initialize configuration
conf = SparkConf() \
    .setAppName("PySpark-Foundations-Lab") \
    .setMaster("local[*]")  # local[*] uses all available CPU cores on the host

# Instantiate SparkContext
sc = SparkContext.getOrCreate(conf=conf)

# Inspect runtime metadata
print("Spark Engine Version :", sc.version)
print("Python Driver Version:", sc.pythonVer)
print("Master URL           :", sc.master)
print("Default Parallelism  :", sc.defaultParallelism)
```

### 1.3 SparkSession (`spark`) — Unified Entry Point
`SparkSession` provides a single unified entry point for DataFrame operations, Spark SQL, Hive integration, and Structured Streaming.

```python
from pyspark.sql import SparkSession

# Initialize a standard SparkSession
spark = SparkSession.builder \
    .appName("PySpark-DataEngine-Session") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

# Retrieve internal SparkContext if low-level RDD APIs are required
sc = spark.sparkContext
print("Active Session App Name:", spark.sparkContext.appName)
```

#### Operational Execution Modes
| Execution Target | Master Parameter | Operational Profile |
| :--- | :--- | :--- |
| **Local Machine / VM** | `master("local[*]")` | Runs all processes inside a single JVM; ideal for rapid development and testing. |
| **YARN Client** | `master("yarn")` | Driver runs on the client machine/VM; executor containers are allocated across the YARN cluster. |
| **YARN Cluster** | Submitted via CLI | Driver is packaged inside the YARN ApplicationMaster on a worker node; ideal for automated production pipelines. |

### 1.4 Command-Line Job Submission (`spark-submit`)
To deploy production Python scripts without launching interactive Jupyter notebooks, use the `spark-submit` utility:

```bash
# Submit standalone PySpark application to local cluster
spark-submit \
    --master local[*] \
    --name "Batch-ETL-Job" \
    --conf spark.sql.shuffle.partitions=4 \
    my_script.py

# Submit to YARN cluster with external PostgreSQL JDBC driver dependency
spark-submit \
    --master yarn \
    --deploy-mode client \
    --jars /opt/spark3/jars/postgresql-42.7.2.jar \
    --num-executors 2 \
    --executor-cores 2 \
    --executor-memory 2G \
    my_etl_pipeline.py
```

---

## 2. Resilient Distributed Datasets (RDDs) Manual

Resilient Distributed Datasets (RDDs) are fault-tolerant, immutable, lazily evaluated collections of records partitioned across worker nodes.

### 2.1 RDD Creation & Partition Inspection
RDDs can be initialized from in-memory Python iterables or ingested from external storage systems (local filesystems, HDFS, or S3).

```python
# 1. Parallelize in-memory collection with explicit partition count
data_list = [10, 20, 30, 40, 50, 60, 70, 80]
rdd_numbers = sc.parallelize(data_list, numSlices=4)

# Inspect partition count and contents per partition
print("Number of Partitions:", rdd_numbers.getNumPartitions())
print("Elements per Partition:", rdd_numbers.glom().collect())

# 2. Ingest external text file
# Note: In local mode, paths point to local files; in cluster mode, HDFS paths are default.
rdd_text = sc.textFile("data.txt", minPartitions=2)
print("Line count in external file:", rdd_text.count())
```

### 2.2 Functional Transformations vs. Actions

Transformations are **lazy** and build a Directed Acyclic Graph (DAG) of dependencies. Actions force computation of the DAG and return values to the driver or write data out.

```python
# Sample text line data
raw_sentences = sc.parallelize([
    "Apache Spark is a unified analytics engine",
    "Spark provides in-memory distributed processing",
    "PySpark enables scalable Python programming"
])

# Transformations (Lazy: no execution occurs here)
# map(): One-to-one transformation
words_mapped = raw_sentences.map(lambda sentence: sentence.split(" "))

# flatMap(): One-to-many transformation (flattens nested arrays)
words_flattened = raw_sentences.flatMap(lambda sentence: sentence.split(" "))

# filter(): Retains elements matching boolean predicate
spark_words = words_flattened.filter(lambda word: "spark" in word.lower())

# Actions (Triggers cluster execution)
print("Collected Matching Words:", spark_words.collect())
print("Total Matching Count    :", spark_words.count())
print("First Matching Word     :", spark_words.first())
print("Top 2 Alphabetical      :", spark_words.top(2))
print("Take First 2 Elements   :", spark_words.take(2))
```

### 2.3 Pair RDDs & Distributed Aggregations
Pair RDDs store key-value tuples `(K, V)`. They enable relational joins, sorting, and key-based aggregations.

```python
# Initialize paired dataset: (department, salary)
salaries = sc.parallelize([
    ("Engineering", 85000),
    ("Marketing", 55000),
    ("Engineering", 95000),
    ("Finance", 72000),
    ("Marketing", 62000)
])

# reduceByKey(): Merges values for each key using an associative and commutative reduce function
# Combines locally on mapper before shuffling (highly efficient)
dept_totals = salaries.reduceByKey(lambda acc, val: acc + val)
print("Department Total Salaries:", dept_totals.collect())

# sortByKey(): Orders records by key
dept_sorted = dept_totals.sortByKey(ascending=True)
print("Alphabetically Sorted:", dept_sorted.collect())

# countByKey(): Action returning Python dictionary of key occurrences
dept_counts = salaries.countByKey()
print("Record counts per dept:", dict(dept_counts))
```

### 2.4 End-to-End Pipeline: Distributed Word Count & Stop-Word Filtering
This classic pipeline demonstrates `flatMap`, `filter`, `map`, `reduceByKey`, and `sortByKey` in sequence.

```python
stop_words = {"is", "a", "the", "in", "and", "of", "to"}

# Read text, normalize, filter stop words, and calculate frequencies
word_counts = sc.textFile("data.txt") \
    .flatMap(lambda line: line.lower().split(" ")) \
    .map(lambda word: word.strip(".,!?:;\"'()[]{}")) \
    .filter(lambda word: len(word) > 0 and word not in stop_words) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .map(lambda tuple_kv: (tuple_kv[1], tuple_kv[0])) \
    .sortByKey(ascending=False) \
    .map(lambda tuple_vk: (tuple_vk[1], tuple_vk[0]))

# Fetch top 10 most frequent terms
for word, count in word_counts.take(10):
    print(f"{word:15s}: {count}")
```

---

## 3. DataFrame Ingestion & Programmatic Schema Engineering

DataFrames organize distributed data into named columns. Defining explicit schemas avoids expensive schema inference passes over datasets and guarantees strict data type integrity.

### 3.1 Converting RDDs to DataFrames
```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# In-memory record tuples
employee_data = [("Alice", "Engineering", 90000), ("Bob", "Sales", 65000), ("Charlie", "Marketing", 70000)]

# Method A: toDF() with default or manual column names
rdd_emp = sc.parallelize(employee_data)
df_emp_quick = rdd_emp.toDF(["Name", "Department", "Salary"])
df_emp_quick.printSchema()

# Method B: createDataFrame() with explicit StructType schema
explicit_schema = StructType([
    StructField("Name", StringType(), False),
    StructField("Department", StringType(), True),
    StructField("Salary", IntegerType(), True)
])

df_emp_formal = spark.createDataFrame(rdd_emp, schema=explicit_schema)
df_emp_formal.show()
```

### 3.2 Reading Tabular Data (CSV & TSV)
```python
# 1. Ingestion with automatic schema inference
df_csv_inferred = spark.read.csv(
    "people.csv",
    header=True,
    inferSchema=True,
    sep=","
)

# 2. Production Ingestion with strict pre-defined schema
voter_schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Title", StringType(), True),
    StructField("Voter_Name", StringType(), True)
])

df_voters = spark.read.csv(
    "DallasCouncilVoters.csv.gz",
    header=True,
    schema=voter_schema,
    sep=","
)
```

### 3.3 DataFrame Inspection & Metadata Introspection
```python
# Print tree representation of columns and datatypes
df_voters.printSchema()

# Retrieve list of column names
print("Columns:", df_voters.columns)

# Retrieve list of (column_name, datatype) tuples
print("Data Types:", df_voters.dtypes)

# Visual inspection of row content
df_voters.show(n=5, truncate=False)

# Total row count
print("Total Records:", df_voters.count())
```

---

## 4. DataFrame Manipulations, Filtering & Data Cleaning

DataFrame transformations allow column projections, computed columns, filtering expressions, and deduplication.

### 4.1 Column Selection, Aliasing & Renaming
```python
from pyspark.sql import functions as F

# Select specific columns
df_subset = df_voters.select("Voter_Name", "Title")

# Select with expression / aliasing
df_aliased = df_voters.select(
    F.col("Voter_Name").alias("Full_Name"),
    F.upper(F.col("Title")).alias("Department_Title")
)

# Rename an individual column
df_renamed = df_voters.withColumnRenamed("Voter_Name", "Council_Member")
```

### 4.2 Adding, Modifying & Dropping Columns
```python
# Add new computed column
df_modified = df_voters.withColumn("Ingestion_Timestamp", F.current_timestamp())

# Modify existing column in-place (e.g. converting text to uppercase)
df_modified = df_modified.withColumn("Title", F.upper(F.col("Title")))

# Drop single or multiple columns
df_clean = df_modified.drop("Ingestion_Timestamp")
```

### 4.3 Row Filtering & Boolean Conditions
PySpark supports both SQL expression strings and programmatic Column object syntax.

```python
# Method A: Column object expressions with bitwise operators (& for AND, | for OR, ~ for NOT)
df_filtered_a = df_voters.filter(
    (F.col("Title") == "MAYOR") & 
    (F.col("Voter_Name").isNotNull())
)

# Method B: SQL expression strings
df_filtered_b = df_voters.where("Title = 'MAYOR' AND Voter_Name LIKE 'Jennifer%'")

# String pattern matching operations
df_pattern = df_voters.filter(F.col("Voter_Name").contains("Smith"))
df_starts = df_voters.filter(F.col("Voter_Name").startswith("Council"))
```

### 4.4 Deduplication & Missing Value Remediation
```python
# Distinct rows across all columns
df_unique = df_voters.distinct()

# Deduplicate based on specific key columns
df_dedup_keys = df_voters.dropDuplicates(subset=["Voter_Name", "Title"])

# Drop rows where any or all specified columns are null
df_no_nulls = df_voters.dropna(how="any", subset=["Voter_Name"])

# Fill null values with default fallback
df_filled = df_voters.fillna({"Title": "UNKNOWN_TITLE", "Voter_Name": "ANONYMOUS"})
```

---

## 5. Advanced Column Operations & Built-in Functions

Real-world ETL requires conditional evaluations, row identifiers, custom business logic, and specialized data type handling (JSON, DateTime, Array, and Window functions).

### 5.1 Conditional Expressions (`when` / `otherwise`)
Chain `when()` and `.otherwise()` to execute multi-condition evaluation similar to SQL `CASE WHEN`.

```python
from pyspark.sql import functions as F

df_voters_categorized = df_voters.withColumn(
    "Leadership_Tier",
    F.when(F.col("Title") == "MAYOR", "Executive")
     .when(F.col("Title") == "DEPUTY MAYOR PRO TEM", "Senior Leadership")
     .when(F.col("Title").contains("COUNCILMEMBER"), "General Council")
     .otherwise("Standard Staff")
)
df_voters_categorized.select("Voter_Name", "Title", "Leadership_Tier").show(5, False)
```

### 5.2 Monotonically Increasing Unique IDs
To assign unique numeric keys across partitions without triggering an expensive shuffle, use `monotonically_increasing_id()`.

```python
df_with_id = df_voters.withColumn("Record_ID", F.monotonically_increasing_id())
df_with_id.select("Record_ID", "Voter_Name").show(5)
```

### 5.3 User-Defined Functions (UDFs)
When native functions are insufficient, Python functions can be registered as UDFs.

```python
from pyspark.sql.types import StringType

# Define standard Python function
def extract_initials(full_name):
    if not full_name:
        return "N/A"
    parts = full_name.strip().split()
    return "".join([p[0].upper() for p in parts if len(p) > 0])

# Register UDF with explicit return type
initials_udf = F.udf(extract_initials, StringType())

# Apply UDF to DataFrame column
df_with_initials = df_voters.withColumn("Initials", initials_udf(F.col("Voter_Name")))
df_with_initials.select("Voter_Name", "Initials").show(5)
```

### 5.4 Semi-Structured JSON Processing
PySpark offers optimized native functions to ingest, unpack, and write JSON records.

```python
from pyspark.sql.types import DoubleType, ArrayType

# Sample dataset containing raw JSON strings
raw_json_records = [
    (1, '{"title": "Inception", "year": 2010, "rating": 8.8, "tags": ["sci-fi", "dream"]}'),
    (2, '{"title": "Interstellar", "year": 2014, "rating": 8.6, "tags": ["sci-fi", "space"]}')
]
df_raw_json = spark.createDataFrame(raw_json_records, ["id", "json_string"])

# 1. from_json(): Parse JSON string into structured columns using StructType
movie_schema = StructType([
    StructField("title", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("tags", ArrayType(StringType()), True)
])

df_parsed = df_raw_json.withColumn("movie_data", F.from_json(F.col("json_string"), movie_schema))
df_parsed.select("id", "movie_data.title", "movie_data.year", "movie_data.tags").show(truncate=False)

# 2. json_tuple(): High-performance extraction of top-level string fields
df_extracted = df_raw_json.select(
    F.col("id"),
    F.json_tuple(F.col("json_string"), "title", "year", "rating").alias("movie_title", "release_year", "imdb_rating")
)
df_extracted.show()

# 3. to_json(): Convert a StructType column back into a single JSON string
df_re_json = df_parsed.select(F.col("id"), F.to_json(F.col("movie_data")).alias("serialized_json"))
df_re_json.show(truncate=False)
```

### 5.5 DateTime Transformations
```python
df_events = spark.createDataFrame([
    ("2024-01-15 10:30:00", "2024-06-20 18:45:00"),
    ("2023-11-01 08:15:00", "2024-03-10 12:00:00")
], ["start_time", "end_time"])

df_dates = df_events \
    .withColumn("start_dt", F.to_date(F.col("start_time"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("end_dt", F.to_date(F.col("end_time"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("days_elapsed", F.datediff(F.col("end_dt"), F.col("start_dt"))) \
    .withColumn("months_elapsed", F.months_between(F.col("end_dt"), F.col("start_dt"))) \
    .withColumn("extended_date", F.add_months(F.col("start_dt"), 3)) \
    .withColumn("formatted_date", F.date_format(F.col("start_dt"), "MMMM dd, yyyy")) \
    .withColumn("start_year", F.year(F.col("start_dt")))

df_dates.select("start_dt", "days_elapsed", "months_elapsed", "formatted_date").show(truncate=False)
```

### 5.6 Array Functions
```python
df_arrays = spark.createDataFrame([
    ("DevA", ["Python", "Spark", "SQL"]),
    ("DevB", ["Java", "Scala"])
], ["Developer", "Tech_Stack"])

# explode(): Creates a new row for each element in the array
df_exploded = df_arrays.withColumn("Technology", F.explode(F.col("Tech_Stack")))
df_exploded.show()

# array_contains(), size(), sort_array()
df_inspected = df_arrays \
    .withColumn("Skill_Count", F.size(F.col("Tech_Stack"))) \
    .withColumn("Knows_Spark", F.array_contains(F.col("Tech_Stack"), "Spark")) \
    .withColumn("Alphabetical_Stack", F.sort_array(F.col("Tech_Stack"), asc=True))

df_inspected.show(truncate=False)
```

### 5.7 Window Functions
Window functions compute calculations across a defined set of rows related to the current row without collapsing the dataset.

```python
from pyspark.sql.window import Window

sales_records = [
    ("North", "Electronics", 5000),
    ("North", "Electronics", 7000),
    ("North", "Furniture", 3000),
    ("South", "Electronics", 8000),
    ("South", "Furniture", 4000),
    ("South", "Furniture", 4500)
]
df_sales = spark.createDataFrame(sales_records, ["Region", "Category", "Amount"])

# Define Window Specification: partitioned by Region and ordered by Amount descending
window_spec = Window.partitionBy("Region").orderBy(F.col("Amount").desc())

# Ranking & Analytical Functions
df_windowed = df_sales \
    .withColumn("Row_Num", F.row_number().over(window_spec)) \
    .withColumn("Rank", F.rank().over(window_spec)) \
    .withColumn("Dense_Rank", F.dense_rank().over(window_spec)) \
    .withColumn("Next_Higher_Amount", F.lead("Amount", 1).over(window_spec)) \
    .withColumn("Previous_Amount", F.lag("Amount", 1).over(window_spec)) \
    .withColumn("Running_Regional_Total", F.sum("Amount").over(Window.partitionBy("Region").orderBy("Amount")))

df_windowed.show(truncate=False)
```

---

## 6. Spark SQL Engine & Relational Database Connectivity (JDBC)

Spark SQL enables execution of ANSI SQL queries on DataFrames and establishes bi-directional connections to relational databases.

### 6.1 Creating Temporary Views & Running SQL
```python
# Register DataFrame as a session-scoped temporary table
df_voters.createOrReplaceTempView("voters_table")

# Execute standard ANSI SQL
sql_result = spark.sql("""
    SELECT 
        Title,
        COUNT(*) AS Total_Voters,
        COUNT(DISTINCT Voter_Name) AS Unique_Voters
    FROM voters_table
    WHERE Title IS NOT NULL
    GROUP BY Title
    ORDER BY Total_Voters DESC
    LIMIT 10
""")

sql_result.show(truncate=False)
```

### 6.2 Relational Database Integration via JDBC (PostgreSQL)
In your VM environment, the PostgreSQL JDBC jar (`postgresql-42.7.2.jar`) resides in `/opt/spark3/jars/` and is automatically loaded onto the classpath.

#### Reading Tables from PostgreSQL
```python
jdbc_url = "jdbc:postgresql://localhost:5432/itversity_hr_db"
connection_properties = {
    "user": "postgres",
    "password": "your_password",
    "driver": "org.postgresql.Driver"
}

# Ingest table into a Spark DataFrame
df_salaries = spark.read.jdbc(
    url=jdbc_url,
    table="salaries",
    properties=connection_properties
)

df_salaries.printSchema()
df_salaries.show(5)
```

#### Writing DataFrames to PostgreSQL
```python
# Save DataFrame back to database
# Save modes: 'append', 'overwrite', 'ignore', 'errorifexists'
df_voters.write.jdbc(
    url=jdbc_url,
    table="processed_voter_summary",
    mode="overwrite",
    properties=connection_properties
)
```

---

## 7. Performance Optimization, Caching & Cluster Tuning

Proper memory management, join strategies, and storage format selection are critical to building production-ready Spark pipelines.

### 7.1 DataFrame Caching & Persistence
When an intermediate DataFrame is evaluated multiple times across downstream calculations, caching it avoids recomputing its upstream lineage graph.

```python
from pyspark.storagelevel import StorageLevel

df_heavy = spark.read.parquet("AA_DFW_2017_Departures_Short.parquet").filter(F.col("Delay") > 15)

# In-memory caching (MEMORY_AND_DISK deserialized default)
df_heavy.cache()

# Trigger materialization via an action
df_heavy.count()
print("Is DataFrame Cached:", df_heavy.is_cached)

# Remove DataFrame from cache once downstream processing finishes
df_heavy.unpersist()
print("Is Cached after unpersist:", df_heavy.is_cached)
```

### 7.2 Storage Formats: Apache Parquet vs. CSV
Parquet provides columnar storage, dictionary encoding, and statistics embedded within file metadata.

```python
# Write DataFrame out as compressed columnar Parquet files
df_voters.write.parquet(
    "output_parquet/voters/",
    mode="overwrite",
    compression="snappy"
)

# Read Parquet files (schemas and column datatypes are retained automatically)
df_from_parquet = spark.read.parquet("output_parquet/voters/")

# Benefit of Predicate Pushdown:
# Spark reads only the required columns and skips unneeded row groups based on metadata
df_fast_query = df_from_parquet.select("Voter_Name").filter("Title = 'MAYOR'")
df_fast_query.show()
```

### 7.3 Tuning Shuffle Partitions & File Import Performance
```python
# Check default shuffle partition count (Default is 200)
current_partitions = spark.conf.get("spark.sql.shuffle.partitions")
print("Default Shuffle Partitions:", current_partitions)

# Tune for local development / small datasets to prevent creating hundreds of empty tasks
spark.conf.set("spark.sql.shuffle.partitions", "4")

# Wildcard import optimization: read partitioned file chunks simultaneously
df_all_chunks = spark.read.csv("AA_DFW_2017_Departures_*.csv.gz", header=True)

# Repartition vs Coalesce
# repartition(n): Performs a full shuffle to evenly distribute data into n partitions (increases or decreases)
df_repartitioned = df_all_chunks.repartition(8)

# coalesce(n): Decreases partition count without a full shuffle by merging local partitions
df_coalesced = df_repartitioned.coalesce(2)
```

### 7.4 Broadcast Hash Join vs. Standard Sort-Merge Join
When joining a large DataFrame with a small lookup table, standard joins incur heavy network shuffle overhead. Broadcasting the small table copies it to every executor memory, eliminating the shuffle phase.

```python
# Ingest large table and small lookup table
df_flights = spark.read.csv("AA_DFW_2018_Departures_Short.csv.gz", header=True, inferSchema=True)
df_airports = spark.read.csv("airportnames.txt.gz", header=True, inferSchema=True)

# 1. Standard Sort-Merge Join (Incurs full cluster shuffle on join key)
df_normal_join = df_flights.join(df_airports, df_flights["Destination Airport"] == df_airports["Airport Code"], how="inner")

# 2. Optimized Broadcast Join
df_broadcast_join = df_flights.join(
    F.broadcast(df_airports),
    df_flights["Destination Airport"] == df_airports["Airport Code"],
    how="inner"
)

# Inspect Physical Execution Plan
# Look for 'BroadcastHashJoin' vs 'SortMergeJoin'
df_broadcast_join.explain()
```

---

## 8. Real-Time Structured Streaming & Apache Kafka Operations

Structured Streaming processes real-time continuous streams using the Spark SQL Catalyst engine under an **unbounded table** paradigm.

### 8.1 Pure Structured Streaming (Socket & File Sources)
```python
# 1. Ingest continuous stream from TCP Socket (Start listener first via: nc -lk 9999)
socket_stream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Tokenize and aggregate word counts
streaming_words = socket_stream.select(F.explode(F.split(F.col("value"), " ")).alias("word"))
word_counts = streaming_words.groupBy("word").count()

# 2. Start Streaming Query to Console Sink
query_socket = word_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime="5 seconds") \
    .start()

# query_socket.awaitTermination()
```

### 8.2 Streaming File Source with Explicit Schema
```python
stock_schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Volume", IntegerType(), True),
    StructField("Symbol", StringType(), True)
])

# Read streaming CSV drops from directory
file_stream = spark.readStream \
    .format("csv") \
    .schema(stock_schema) \
    .option("header", "true") \
    .option("maxFilesPerTrigger", 1) \
    .load("data/stream/")

# Filter stream
high_volume_stream = file_stream.filter(F.col("Volume") > 500000)

# Write output to Parquet with fault-tolerant checkpointing
query_file = high_volume_stream.writeStream \
    .format("parquet") \
    .option("path", "output_stream/parquet/") \
    .option("checkpointLocation", "output_stream/checkpoints/") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()
```

### 8.3 Time Windows & Watermarking
To process late-arriving event data and prevent infinite state retention in memory, define event-time watermarks and window aggregations.

```python
# Tumbling or Sliding Window Aggregations
windowed_counts = file_stream \
    .withColumn("Event_Timestamp", F.to_timestamp(F.col("Date"), "yyyy-MM-dd")) \
    .withWatermark("Event_Timestamp", "10 minutes") \
    .groupBy(
        F.window(F.col("Event_Timestamp"), windowDuration="1 hour", slideDuration="30 minutes"),
        F.col("Symbol")
    ) \
    .agg(F.avg("Close").alias("Average_Closing_Price"))
```

### 8.4 Apache Kafka Integration
Kafka delivers high-throughput publish-subscribe message streams.

#### Terminal Commands: Kafka Broker & Producer Administration
```bash
# 1. Start ZooKeeper and Kafka Broker
zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties &
kafka-server-start.sh /opt/kafka/config/server.properties &

# 2. Create Topic with 3 partitions and replication factor of 1
kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 3 \
    --topic voter_stream_topic

# 3. Verify created topics
kafka-topics.sh --list --bootstrap-server localhost:9092

# 4. Stream data into topic using Console Producer or custom script
python3 pushKafkaData.sh
```

#### PySpark Script: Ingesting & Parsing Kafka Streams
```python
# Read binary Kafka stream
kafka_raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "voter_stream_topic") \
    .option("startingOffsets", "latest") \
    .load()

# In Kafka, 'key' and 'value' arrive as binary arrays (bytea). Cast to string:
kafka_string_stream = kafka_raw_stream.selectExpr(
    "CAST(key AS STRING) AS message_key",
    "CAST(value AS STRING) AS message_value",
    "timestamp AS kafka_timestamp"
)

# Unpack embedded JSON message value
voter_json_schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Title", StringType(), True),
    StructField("Voter_Name", StringType(), True)
])

parsed_kafka_stream = kafka_string_stream \
    .withColumn("data", F.from_json(F.col("message_value"), voter_json_schema)) \
    .select("kafka_timestamp", "data.Date", "data.Title", "data.Voter_Name")

# Write stream out to Console or database sink
kafka_query = parsed_kafka_stream.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/kafka_checkpoint/") \
    .start()
```

---

## 9. Distributed Machine Learning with Spark MLlib

Spark MLlib provides distributed machine learning algorithms structured around DataFrames, Transformers, Estimators, and Pipelines.

### 9.1 Feature Engineering with `VectorAssembler`
Machine learning algorithms in Spark require numerical feature columns to be combined into a single feature vector column.

```python
from pyspark.ml.feature import VectorAssembler

# Sample feature dataset
df_housing = spark.createDataFrame([
    (250000, 3, 2.0, 1800),
    (380000, 4, 3.0, 2400),
    (195000, 2, 1.0, 1100)
], ["Price", "Bedrooms", "Bathrooms", "SqFt"])

# Assemble feature columns into a vector
assembler = VectorAssembler(
    inputCols=["Bedrooms", "Bathrooms", "SqFt"],
    outputCol="features"
)
df_features = assembler.transform(df_housing)
df_features.select("features", "Price").show(truncate=False)
```

### 9.2 The Three Core Algorithms ("The Three C's")

#### 1. Collaborative Filtering (`ALS` for Recommendations)
Alternating Least Squares (ALS) models user-item interactions for building distributed recommendation engines.

```python
from pyspark.ml.recommendation import ALS

# Ratings format: (userID, movieID, rating)
df_ratings = spark.createDataFrame([
    (0, 101, 4.0), (0, 102, 5.0),
    (1, 101, 2.0), (1, 103, 3.5),
    (2, 102, 4.5), (2, 103, 5.0)
], ["userId", "movieId", "rating"])

# Train ALS model
als = ALS(
    maxIter=10,
    regParam=0.01,
    userCol="userId",
    itemCol="movieId",
    ratingCol="rating",
    coldStartStrategy="drop"
)
model_als = als.fit(df_ratings)

# Generate top 2 movie recommendations for all users
user_recs = model_als.recommendForAllUsers(numItems=2)
user_recs.show(truncate=False)
```

#### 2. Classification (`LogisticRegression`)
Binary and multiclass prediction models.

```python
from pyspark.ml.classification import LogisticRegression

# Sample classification training data
df_classified = spark.createDataFrame([
    (1.0, [2.5, 3.1]),
    (0.0, [0.5, 1.2]),
    (1.0, [3.0, 4.2]),
    (0.0, [0.2, 0.8])
], ["label", "features"])

# Train Logistic Regression model
lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
model_lr = lr.fit(df_classified)
predictions = model_lr.transform(df_classified)
predictions.select("features", "label", "prediction", "probability").show(truncate=False)
```

#### 3. Clustering (`KMeans`)
Unsupervised partitioning of data points into k clusters based on geometric distance.

```python
from pyspark.ml.clustering import KMeans

# Train KMeans cluster model
kmeans = KMeans(k=2, seed=1)
model_kmeans = kmeans.fit(df_features)

# Inspect cluster centers
centers = model_kmeans.clusterCenters()
print("Cluster Centers:", centers)
```

---

## 10. Comprehensive Command & Operation Quick-Reference Index

| Operation Category | PySpark Method / Command | Functional Purpose |
| :--- | :--- | :--- |
| **Context & Session** | `SparkContext.getOrCreate()` | Initializes low-level cluster context |
| | `SparkSession.builder.getOrCreate()` | Initializes unified SparkSession |
| **RDD Operations** | `sc.parallelize(iterable, numSlices)` | Distributes local collection into RDD |
| | `sc.textFile(path, minPartitions)` | Loads external text file into RDD |
| | `rdd.map(lambda x: ...)` | 1-to-1 transformation across elements |
| | `rdd.flatMap(lambda x: ...)` | 1-to-many transformation with output flattening |
| | `rdd.filter(lambda x: ...)` | Retains elements matching boolean condition |
| | `rdd.reduceByKey(lambda a, b: ...)` | Merges key-value pairs with local map-side combine |
| | `rdd.sortByKey(ascending=True)` | Sorts paired RDD by key |
| | `rdd.countByKey()` | Returns dictionary of record counts per key |
| **DataFrame Creation** | `spark.createDataFrame(rdd, schema)` | Converts RDD to DataFrame with explicit schema |
| | `spark.read.csv(path, header, schema)` | Ingests delimited text data |
| | `spark.read.parquet(path)` | Ingests columnar Parquet dataset |
| **DataFrame Inspection** | `df.printSchema()` | Outputs tree representation of schema |
| | `df.show(n, truncate=False)` | Renders top n records in tabular ASCII format |
| | `df.dtypes` / `df.columns` | Returns column datatypes and column names list |
| | `df.count()` | Returns total row count across partitions |
| **Transformations** | `df.select(*cols)` | Projects subset of columns or expressions |
| | `df.withColumn(colName, colExpr)` | Adds new column or updates existing column |
| | `df.withColumnRenamed(old, new)` | Renames single column |
| | `df.drop(*colNames)` | Removes column(s) from schema |
| | `df.filter(condition)` / `df.where()` | Filters rows by boolean expression |
| | `df.distinct()` / `dropDuplicates()` | Removes duplicate rows |
| **Specialized Functions** | `F.when(c, val).otherwise(d)` | Evaluates conditional branching |
| | `F.monotonically_increasing_id()` | Generates partitioned unique numeric IDs |
| | `F.udf(python_func, returnType)` | Registers Python function as custom Spark UDF |
| | `F.from_json(col, schema)` | Deserializes JSON string into StructType |
| | `F.json_tuple(col, *fields)` | Extracts multiple fields from JSON string |
| | `F.to_json(col)` | Serializes complex StructType column into JSON string |
| | `F.explode(col)` | Unrolls array elements into separate rows |
| | `Window.partitionBy().orderBy()` | Defines analytical window partition specification |
| | `F.row_number().over(winSpec)` | Assigns sequential row numbers per window partition |
| **Optimization & Tuning**| `df.cache()` / `df.persist()` | Stores DataFrame in memory/disk cache |
| | `df.unpersist()` | Releases cached DataFrame from memory |
| | `df.repartition(n)` | Full network shuffle into n partitions |
| | `df.coalesce(n)` | Merges local partitions to reduce count without shuffle |
| | `F.broadcast(df_small)` | Marks small DataFrame for Broadcast Hash Join |
| | `df.explain(extended=True)` | Prints logical and physical execution plans |
| **Streaming & Kafka** | `spark.readStream.format("socket")` | Streams text records over TCP connection |
| | `spark.readStream.format("kafka")` | Streams event messages from Apache Kafka |
| | `writeStream.outputMode("append")` | Sets streaming output mode (append/complete/update) |
| | `writeStream.trigger(...)` | Sets micro-batch execution interval |
| | `df.withWatermark(col, delay)` | Purges late-arriving state beyond threshold |
| **Machine Learning** | `VectorAssembler(inputCols, outputCol)` | Merges numerical columns into single feature vector |
| | `ALS(userCol, itemCol, ratingCol)` | Trains distributed collaborative filtering model |
| | `LogisticRegression(maxIter, regParam)` | Fits binary or multiclass classification model |
| | `KMeans(k, seed)` | Trains unsupervised clustering model |
