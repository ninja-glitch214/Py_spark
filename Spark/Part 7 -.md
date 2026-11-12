# Part 7 – Storage, Execution and Performance Optimization

Objective: Understand how Spark stores, reads, partitions, joins, and executes data efficiently.

This part builds on concepts already covered: RDD persistence, partitions, shuffle, lazy evaluation, Catalyst, and the Driver–Executor architecture. I’ll avoid repeating their fundamentals and focus on how to use them for performance optimization.

## A. Caching and persistence

You have already covered the basic purpose of `cache()`, `persist()`, and `unpersist()` in Part 4. Here, the focus is on choosing the right storage level and checking whether caching is actually useful.

### 1. `cache()` vs `persist()`

|Method|Purpose|
|---|---|
|`cache()`|Marks a DataFrame/RDD for caching using its default storage level.|
|`persist()`|Allows you to select a storage level.|
|`unpersist()`|Removes cached/persisted data and releases its storage resources.|
|`is_cached`|Checks whether a DataFrame is marked for caching.|

```
df.cache()
print(df.is_cached)       # True

df.unpersist()
print(df.is_cached)       # False
```

`cache()` and `persist()` are lazy: the data is generally materialized when an action executes.

### 2. Storage levels

A storage level determines:

- Whether data is stored in memory, disk, or both.
    
- Whether it is stored in serialized or deserialized form.
    
- Whether replicas are maintained.
    

Common PySpark storage levels:

|Storage level|Meaning|
|---|---|
|`MEMORY_ONLY`|Store deserialized partitions in memory; unavailable partitions may be recomputed.|
|`MEMORY_AND_DISK`|Store in memory; partitions that do not fit spill to disk.|
|`MEMORY_ONLY_2`|Memory-only storage with two replicas.|
|`MEMORY_AND_DISK_2`|Memory and disk storage with two replicas.|
|`DISK_ONLY`|Store partitions on disk.|

Example:

```
from pyspark import StorageLevel

df.persist(StorageLevel.MEMORY_AND_DISK)
df.count()  # Materializes the persisted DataFrame
```

For DataFrames, `cache()` uses `MEMORY_AND_DISK_DESER` by default in modern Spark versions. RDD `cache()` uses `MEMORY_ONLY` by default. Do not assume both APIs have identical defaults.

Serialized vs deserialized

- Deserialized: Objects are stored in their normal in-memory representation. Access can be faster, but memory usage can be higher.
    
- Serialized: Objects are stored as serialized binary data. This can reduce memory usage, but requires serialization/deserialization during access.
    

When to persist: Use persistence when the same expensive intermediate dataset is reused multiple times. Caching a dataset used only once can waste executor memory.

## B. File formats and storage

### 1. Row-oriented vs columnar storage

Row-oriented storage

Stores values belonging to one record together.

```
Row 1 → 101, Alice, Pune
Row 2 → 102, Bob, Mumbai
Row 3 → 103, Carol, Delhi
```

Suitable when applications frequently read or update complete records.

Columnar storage

Stores values belonging to the same column together.

```
ID     → 101, 102, 103
Name   → Alice, Bob, Carol
City   → Pune, Mumbai, Delhi
```

Suitable for analytical workloads that read selected columns from large datasets.

### 2. CSV vs Parquet vs ORC

|Feature|CSV|Parquet|ORC|
|---|---|---|---|
|Storage|Text, row-oriented|Columnar|Columnar|
|Schema|Not inherently stored|Stored|Stored|
|Data types|Mostly represented as text|Preserved|Preserved|
|Compression|Possible, often external|Supported|Supported|
|Analytical queries|Less efficient|Efficient|Efficient|
|Human-readable|Yes|No|No|

CSV

- Simple text format, commonly used for data exchange.
    
- Requires parsing and often schema inference or an explicitly supplied schema.
    
- Reading selected columns may still require scanning and parsing records.
    
- Larger storage footprint compared with compressed columnar formats in many analytical workloads.
    

Parquet

- Columnar format widely used in Spark and data lake systems.
    
- Stores schema and column data in a structured binary format.
    
- Supports compression and encoding.
    
- Efficient when queries access only a few columns.
    

ORC (Optimized Row Columnar)

- Columnar format designed for analytical processing.
    
- Supports compression, indexes, and statistics.
    
- Commonly associated with Hive, but also supported by Spark.
    

### 3. Reading and writing formats

```
# Read CSV
df = spark.read.option("header", True) \
               .option("inferSchema", True) \
               .csv("input/data.csv")

# Write Parquet
df.write.mode("overwrite").parquet("output/parquet_data")

# Read Parquet
parquet_df = spark.read.parquet("output/parquet_data")

# Write ORC
df.write.mode("overwrite").orc("output/orc_data")

# Read ORC
orc_df = spark.read.orc("output/orc_data")
```

Generic format API:

```
df.write.format("parquet").save("output/data")
df = spark.read.format("parquet").load("output/data")
```

Common write modes:

|Mode|Behaviour|
|---|---|
|`append`|Add data to existing output.|
|`overwrite`|Replace existing output.|
|`ignore`|Do nothing if output already exists.|
|`error` / `errorifexists`|Raise an error if output exists; default behaviour.|

### 4. Why columnar storage is efficient

Suppose a dataset contains 50 columns, but a query needs only `name` and `salary`.

With a columnar format, Spark can read the required columns rather than reading every column. This reduces disk I/O and data processing.

Two important optimizations:

Column pruning

- Spark reads only the columns required by the query.
    
- Example:
    

```
df.select("name", "salary").show()
```

Spark may avoid reading the other columns from a columnar file.

Predicate pushdown

- Spark pushes supported filter conditions closer to the data source.
    
- This allows the source reader to skip irrelevant row groups or data blocks using available statistics.
    

```
df.filter(df.salary > 50000).show()
```

For Parquet, statistics such as minimum and maximum values can help skip row groups that cannot satisfy the filter.

Important distinction: Column pruning reduces the number of columns read; predicate pushdown helps reduce the amount of data read by applying filters at the source. Actual optimization depends on the file format, query, and data source support.

### 5. Compression

Compression reduces storage size and often reduces disk/network I/O, at the cost of CPU work for compression and decompression.

Common codecs:

|Codec|Important property|
|---|---|
|Snappy|Fast compression/decompression; common with Parquet.|
|Gzip|Higher compression in many cases, but generally slower; not splittable in its usual form.|
|LZ4|Designed for very fast compression/decompression.|
|Zstandard (ZSTD)|Good balance of compression ratio and speed.|
|BZip2|Splittable, but generally slower.|

Example:

```
df.write \
  .option("compression", "snappy") \
  .parquet("output/compressed_data")
```

Compression support and defaults depend on the format and Spark version.

### 6. Small files problem

Problem: Having thousands of tiny files can make a Spark job inefficient even when the total data size is not large.

Why?

- File listing and metadata operations increase.
    
- Spark may create many small input tasks.
    
- Scheduling overhead becomes significant.
    
- NameNode metadata pressure can increase when files are stored in HDFS.
    

Example:

```
Instead of:
file1 = 10 KB
file2 = 15 KB
file3 = 8 KB
...
file10000 = 12 KB

Prefer:
A smaller number of reasonably sized files.
```

File sizing and output partitions

The number of output files is usually related to the number of partitions writing the data.

```
# Write using current partition count
df.write.parquet("output/data")

# Reduce output partitions before writing
df.coalesce(4).write.parquet("output/data")

# Increase or redistribute partitions
df.repartition(8).write.parquet("output/data")
```

There is no universal ideal file size. For many analytical workloads, files in the hundreds of MB range are a useful starting point, but actual sizing depends on the storage system, workload, and execution environment.

## C. Partitioning and configuration

### 1. Spark SQL shuffle partitions

`spark.sql.shuffle.partitions` controls the default number of partitions used for many shuffle-based SQL operations, such as joins and aggregations.

Default in many Spark versions: 200.

Check it:

```
spark.conf.get("spark.sql.shuffle.partitions")
```

Change it:

```
spark.conf.set("spark.sql.shuffle.partitions", "8")
```

Example:

```
result = df.groupBy("department").count()
```

The shuffle stage may use the configured number of shuffle partitions.

Important:

- This setting does not directly control every RDD partition or every input partition.
    
- It is not necessarily the final number of output files.
    
- A very high value can create many tiny tasks; a very low value can create oversized partitions and long-running tasks.
    
- Adaptive Query Execution (AQE) can adjust shuffle partitioning at runtime when enabled.
    

### 2. `spark.conf.get()` and `spark.conf.set()`

```
# Read a Spark SQL configuration
spark.conf.get("spark.sql.shuffle.partitions")

# Change a runtime SQL configuration
spark.conf.set("spark.sql.shuffle.partitions", "10")
```

To check AQE:

```
spark.conf.get("spark.sql.adaptive.enabled")
```

To enable it:

```
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

`spark.conf` is used for runtime SQL configurations. Not every Spark configuration can be changed after the Spark application has started.

### 3. `repartition()` vs `coalesce()`

Both methods change the number of DataFrame partitions, but their behaviour differs.

`repartition()`

- Can increase or decrease partitions.
    
- Redistributes data across partitions.
    
- Usually involves a shuffle.
    
- Can repartition using one or more columns.
    

```
# Increase partitions to 8
df2 = df.repartition(8)

# Redistribute based on department
df3 = df.repartition(8, "department")
```

`coalesce()`

- Primarily used to reduce partitions.
    
- Avoids a full shuffle in its usual narrow-dependency operation.
    
- May produce uneven partition sizes.
    
- Does not redistribute data as evenly as repartitioning.
    

```
# Reduce partitions to 4
df2 = df.coalesce(4)
```

|Feature|`repartition()`|`coalesce()`|
|---|---|---|
|Increase partitions|Yes|No, not in the normal use case|
|Decrease partitions|Yes|Yes|
|Full shuffle|Usually|Usually avoided|
|Distribution|Redistributes data|Combines existing partitions|
|Typical use|Balance data / increase parallelism|Reduce output files cheaply|

Exam trap: `coalesce()` is not a guaranteed equal-sized partitioning operation. Use `repartition()` when redistribution is important.

### 4. Logical and physical execution plans

Spark converts a DataFrame query into execution plans.

Query execution flow

DataFrame / SQL query

Unresolved logical plan

Names and references are resolved.

Optimized logical plan

Catalyst applies logical optimizations.

Physical plan

Spark selects physical operators and strategies.

Execution

Tasks run across executors.

Logical plan: Describes what operations are required, not exactly how they will be executed.

Physical plan: Describes how Spark intends to execute the query using physical operators and strategies.

### 5. `explain()`

Use `explain()` to inspect a DataFrame's execution plan.

```
df.filter(df.salary > 50000) \
  .select("name", "salary") \
  .explain()
```

Useful modes:

```
df.explain(mode="simple")
df.explain(mode="extended")
df.explain(mode="formatted")
df.explain(mode="cost")
```

|Mode|Purpose|
|---|---|
|`simple`|Physical plan.|
|`extended`|Parsed, analyzed, optimized logical plans and physical plan.|
|`formatted`|Detailed physical plan with structured node information.|
|`cost`|Logical plan with available statistics.|

Look for operators such as:

- `FileScan` – reading data from a file source.
    
- `Filter` – filtering rows.
    
- `Project` – selecting or computing columns.
    
- `Exchange` – data redistribution, commonly associated with shuffle.
    
- `Sort` – sorting.
    
- `BroadcastHashJoin` – broadcast-based join.
    
- `SortMergeJoin` – sort-merge join.
    

Exam/practical point: `explain()` does not execute the complete query merely to show the plan. The actual runtime plan can differ when adaptive execution changes it.

### 6. Data skew

Data skew occurs when some partitions contain significantly more data than others.

Example: joining sales data by `customer_id`, where one customer has an exceptionally large number of records.

```
Partition 1 → 10,000 rows
Partition 2 → 12,000 rows
Partition 3 → 11,000 rows
Partition 4 → 900,000 rows  ← Skewed
```

Effects:

- Some tasks finish quickly while one task runs much longer.
    
- Executor resources remain occupied by slow tasks.
    
- Shuffle data becomes uneven.
    
- Overall job completion is delayed.
    

Possible solutions:

- Repartition using a more suitable key.
    
- Use broadcast joins when one side is small enough.
    
- Enable AQE skew-join optimization.
    
- Use key salting for severe skew in suitable workloads.
    
- Filter or separately process extreme keys where logically valid.
    

AQE setting:

```
spark.conf.set(
    "spark.sql.adaptive.skewJoin.enabled",
    "true"
)
```

Key distinction: Increasing the number of partitions alone does not necessarily solve skew. If a hot key sends most records to one partition, that partition can remain overloaded.

## D. Join strategies

### 1. Join types

Consider two DataFrames:

Employees

|emp_id|name|dept_id|
|---|---|---|
|1|Alice|10|
|2|Bob|20|
|3|Carol|30|

Departments

|dept_id|department|
|---|---|
|10|HR|
|20|IT|
|40|Finance|

Join types determine which matching and non-matching rows are retained.

|Join|Result|
|---|---|
|Inner|Only matching rows.|
|Left outer|All left rows; unmatched right-side values become NULL.|
|Right outer|All right rows; unmatched left-side values become NULL.|
|Full outer|All rows from both sides; unmatched values become NULL.|

PySpark examples:

```
employees.join(departments, "dept_id", "inner")

employees.join(departments, "dept_id", "left")

employees.join(departments, "dept_id", "right")

employees.join(departments, "dept_id", "full")
```

`"full_outer"` and `"full"` are both accepted aliases. Similarly, `"left_outer"` and `"right_outer"` are valid join-type names.

When joining on differently named columns:

```
employees.join(
    departments,
    employees.dept_id == departments.dept_id,
    "inner"
)
```

If both DataFrames contain columns with the same name and you join using an explicit condition, both key columns can remain in the result. Use `select()` to choose or rename the required columns.

### 2. Why join strategy matters

A join may require data to be redistributed between executors. This can cause network transfer, disk I/O, sorting, and memory pressure.

Spark selects a physical join strategy based on factors such as:

- Table sizes and statistics.
    
- Join condition.
    
- Available memory.
    
- Configuration settings.
    
- Adaptive Query Execution.
    

Two important strategies are Sort Merge Join and Broadcast Hash Join.

### 3. Sort Merge Join (SMJ)

Common for large joins

Used for suitable equi-joins, especially when both sides are large.

Conceptual execution:

```
Left DataFrame                 Right DataFrame
      |                              |
   Shuffle                         Shuffle
      |                              |
 Sort by key                     Sort by key
      |                              |
      └──────────┬───────────────────┘
                 |
          Merge matching keys
                 |
             Join result
```

Steps:

1. Rows are redistributed according to the join key.
    
2. Each side is sorted by that key.
    
3. Spark merges the sorted datasets and produces matching rows.
    

Advantages

- Suitable for large datasets.
    
- Does not require the entire smaller dataset to fit into each executor's memory.
    

Disadvantages

- Shuffle and sorting can be expensive.
    
- Data skew can cause uneven task execution.
    

### 4. Broadcast Hash Join (BHJ)

Useful when one side is small

When one DataFrame is sufficiently small, Spark can send a copy of it to each executor rather than shuffling both datasets.

```
                 Small DataFrame
                        |
                    Broadcast
                        |
          ┌─────────────┼─────────────┐
          ↓             ↓             ↓
      Executor 1    Executor 2    Executor 3
          ↑             ↑             ↑
      Large data    Large data    Large data
          |             |             |
       Local join    Local join    Local join
```

Advantages:

- Avoids shuffling the large side for the join.
    
- Avoids sorting both sides as required by SMJ.
    
- Often faster when the broadcast side is small enough.
    

Disadvantages:

- The broadcast dataset must fit comfortably in memory on each executor.
    
- A dataset that is too large can cause memory pressure or broadcast failures.
    

### 5. `broadcast()`

Use `pyspark.sql.functions.broadcast()` to mark a DataFrame as suitable for broadcast joining.

```
from pyspark.sql.functions import broadcast

result = employees.join(
    broadcast(departments),
    "dept_id",
    "inner"
)
```

This tells Spark to use a broadcast join strategy when supported by the query.

To inspect the selected strategy:

```
result.explain()
```

Look for:

```
BroadcastHashJoin
```

or:

```
SortMergeJoin
```

Automatic broadcast: Spark can also choose a broadcast join without explicitly calling `broadcast()`, based on its size estimates and the `spark.sql.autoBroadcastJoinThreshold` configuration.

Check the threshold:

```
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
```

In many Spark versions, the default threshold is 10 MB. A value of `-1` disables automatic broadcasting.

### 6. Choosing a join strategy

|Situation|Suitable strategy|
|---|---|
|Both datasets are large|Often Sort Merge Join|
|One dataset is small enough to broadcast|Broadcast Hash Join|
|Join key has severe skew|Consider AQE skew handling or another skew strategy|
|Join is not an equi-join|Strategy depends on the condition; other join operators may be used|

Exam traps

- Broadcast Hash Join avoids shuffling the large side for the join; it does not mean no data movement or no memory cost.
    
- Sort Merge Join generally requires shuffle and sorting for both sides.
    
- Calling `broadcast()` is a hint, not a guarantee that every possible join condition will use Broadcast Hash Join.
    
- Spark's chosen strategy should be verified with `explain()`.
    

## E. Spark execution optimization

### 1. Catalyst Optimizer

Catalyst is Spark SQL's query optimization framework.

Its job is to transform a query into an efficient execution plan while preserving its meaning.

Important optimization examples:

|Optimization|What it does|
|---|---|
|Predicate pushdown|Pushes supported filters closer to the data source.|
|Column pruning|Removes unnecessary columns from the read and processing plan.|
|Constant folding|Evaluates constant expressions during optimization.|
|Boolean simplification|Simplifies logical expressions.|
|Projection pruning|Removes unnecessary projections.|
|Join reordering|May change join order when statistics and conditions allow it.|

Example:

```
result = (
    df.select("name", "salary", "department")
      .filter("salary > 50000")
      .select("name", "department")
)
```

Spark can recognize that `salary` is needed for filtering but does not need to be part of the final output. It may also push the filter closer to the source.

Important: Catalyst optimizes structured queries. It cannot generally inspect and optimize arbitrary Python logic inside a regular Python UDF in the same way it optimizes built-in Spark expressions.

### 2. Physical execution plans

The physical plan determines which execution operators Spark uses.

Examples:

|Physical operator|Purpose|
|---|---|
|`FileScan`|Reads data from a file source.|
|`Filter`|Filters records.|
|`Project`|Selects or computes columns.|
|`HashAggregate`|Performs hash-based aggregation.|
|`Sort`|Sorts records.|
|`Exchange`|Redistributes data, commonly through shuffle.|
|`BroadcastExchange`|Prepares data for broadcast.|
|`BroadcastHashJoin`|Executes a broadcast hash join.|
|`SortMergeJoin`|Executes a sort-merge join.|

For optimization, inspect whether the plan contains unnecessary exchanges, expensive sorts, or unexpected join strategies.

### 3. Memory management

Spark uses executor memory for multiple purposes:

- Execution memory: Used by joins, aggregations, sorts, and shuffle-related operations.
    
- Storage memory: Used for cached and persisted datasets.
    
- Other memory: JVM overhead, user objects, and other runtime requirements.
    

Execution and storage memory share a unified memory region in modern Spark's memory-management model. They can borrow from each other within configured limits.

Common memory-related problems:

|Problem|Possible reason|
|---|---|
|Out-of-memory error|Dataset or intermediate data exceeds available memory.|
|Excessive garbage collection|Too many objects or high memory pressure.|
|Spill to disk|Execution data does not fit in memory.|
|Executor lost|Executor process crashed or was terminated, possibly due to resource exhaustion.|
|Broadcast failure|Broadcast data is too large or cannot be distributed successfully.|

Possible improvements:

- Avoid unnecessary caching.
    
- Select only required columns.
    
- Filter early when appropriate.
    
- Use suitable partition sizes.
    
- Avoid collecting large datasets to the Driver.
    
- Use efficient file formats such as Parquet.
    
- Broadcast only suitably small datasets.
    

### 4. Shuffle optimization

Shuffle is expensive because it can involve serialization, network transfer, disk I/O, and sorting.

Practical techniques:

1. Reduce unnecessary wide transformations.
    
2. Filter data before joins and aggregations where logically valid.
    
3. Select only required columns before a shuffle.
    
4. Choose a suitable shuffle partition count.
    
5. Use broadcast joins for sufficiently small datasets.
    
6. Handle skewed keys.
    
7. Use AQE where supported.
    

AQE can optimize parts of a query at runtime using actual execution statistics.

Check whether it is enabled:

```
spark.conf.get("spark.sql.adaptive.enabled")
```

Useful AQE configurations:

```
spark.conf.set("spark.sql.adaptive.enabled", "true")

spark.conf.set(
    "spark.sql.adaptive.coalescePartitions.enabled",
    "true"
)

spark.conf.set(
    "spark.sql.adaptive.skewJoin.enabled",
    "true"
)
```

AQE can:

- Coalesce small shuffle partitions.
    
- Optimize certain skewed joins.
    
- Change certain join strategies at runtime.
    

### 5. Executor utilization

Having many executors does not automatically mean a job will run faster.

Performance depends on:

- Number of available CPU cores.
    
- Number and size of partitions.
    
- Number of concurrent tasks.
    
- Memory available to each executor.
    
- Shuffle and data-skew overhead.
    

For example:

```
Available executor cores = 8
Runnable tasks             = 20

At most 8 tasks can run concurrently
(assuming all 8 cores are available for tasks).
```

Too few partitions can underutilize available cores. Too many tiny partitions can increase task-scheduling overhead.

### 6. Spark UI and application monitoring

The Spark Web UI helps diagnose where time and resources are being spent.

[Web UI - Spark 4.1.2 Documentation](https://images.openai.com/static-rsc-4/rncHY6IKQONoNLgzTBxA1ETrVMuilZTOaXaSAIHI2JvFguL25ZaZ6x0zvI2RthhOfsZpjrwpbhzIubaSQHKhQrmu9xwf6Rqvksu1OCyVpCSiipIvUjsPK34ET4xuD_tDAJt14zf8xbZeyMOEelzwqdFIvKiiupK0bKC1HO1_o4s?purpose=inline)

[Web UI - Spark 4.1.2 Documentation](https://images.openai.com/static-rsc-4/EevLiTsYTNt5YQv_Dylu3lRsMjXRZZBnV-BylTfa32TIAfhOAy97BljvQ9UeqJTC5qrNy-0VN7YB4Rk3PUDBjfEIZIyHeNkkrALlH7YnL-5l7qHocJ9U9kgz-iVmtQ32Ft2PlL53BNLIIKRZrCLKYR00JUjhPnUo8hMfOXDUOJs?purpose=inline)

[Web UI - Spark 3.0.0 Documentation](https://images.openai.com/static-rsc-4/PUro-h-PawsHKs4U6e5-tjHoBGX15HFudcpIKDsbvKwp7tjUoTorPifUmXM1DnsUULQOQPOsu0DXCvjKx69X4lX4sIIfJTgJu6CbZ-rKWKSN_gxQAYBDznAPitv-SgPu6390KrAozMdOmEXyv4FA4WsTD2d0q2KEMdDn9skm-YM?purpose=inline)

Common UI tabs:

|Tab|What to inspect|
|---|---|
|Jobs|Job status, duration, and associated stages.|
|Stages|Task duration, shuffle read/write, spills, failed tasks.|
|Storage|Cached RDDs and DataFrames, memory usage.|
|Executors|Executor memory, task counts, failures, GC time.|
|SQL / DataFrame|Query execution details and physical plans.|
|Environment|Spark properties and configuration values.|

Typical local Spark UI address:

```
http://localhost:4040
```

If multiple Spark applications run on the same machine, the UI may use ports 4041, 4042, and so on.

The UI is generally available while the application is running; event logs can be used to inspect completed applications if event logging and a history server are configured.

### 7. Debugging and troubleshooting

|Symptom|What to investigate|
|---|---|
|Job is unexpectedly slow|Stages, task duration, shuffle, skew, and executor utilization.|
|One task takes much longer than others|Data skew or uneven partition sizes.|
|Too many tiny tasks|Excessive partitions or small input files.|
|High shuffle read/write|Join or aggregation strategy, partitioning, and unnecessary exchanges.|
|High spill to disk|Memory pressure or oversized partitions.|
|Out-of-memory error|Executor/Driver memory, caching, large partitions, broadcast size.|
|DataFrame query reads too much data|Column pruning, predicate pushdown, and file format.|
|Cached data is not helping|Check whether it is reused and whether it has been materialized.|

A useful troubleshooting sequence:

```
Job is slow
    |
    v
Open Spark UI
    |
    v
Identify the slow stage
    |
    v
Inspect task duration and shuffle metrics
    |
    +---- One/few slow tasks? → Investigate skew
    |
    +---- High shuffle?        → Investigate joins/aggregations
    |
    +---- High spill/GC?       → Investigate memory and partitions
    |
    +---- Many tiny tasks?     → Investigate partition/file sizes
    |
    v
Inspect physical plan using explain()
    |
    v
Apply a targeted optimization
```

## High-value exam distinctions

|Concept|Remember|
|---|---|
|`cache()`|Uses default storage level.|
|`persist()`|Allows storage-level selection.|
|`unpersist()`|Releases persisted data.|
|Parquet / ORC|Columnar formats.|
|CSV|Text-based, row-oriented format.|
|Column pruning|Reads only required columns.|
|Predicate pushdown|Pushes filters toward the data source.|
|`repartition()`|Redistributes data; usually shuffles.|
|`coalesce()`|Reduces partitions without a full shuffle in its normal use.|
|`spark.sql.shuffle.partitions`|Default shuffle partition count for many SQL operations.|
|`explain()`|Displays query execution plans.|
|Data skew|Uneven distribution of records across partitions.|
|Sort Merge Join|Shuffle and sort both sides for suitable joins.|
|Broadcast Hash Join|Broadcasts the small side to executors.|
|Catalyst|Optimizes structured queries.|
|AQE|Uses runtime statistics to adapt execution.|
|Spark UI|Helps identify slow stages, skew, spills, and resource problems.|

Part 7 is covered.