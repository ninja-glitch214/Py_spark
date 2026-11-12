
# Part 4 – RDDs (Resilient Distributed Datasets)

## 1. What is an RDD?

RDD (Resilient Distributed Dataset) is Spark's fundamental distributed data abstraction.

It represents a collection of data distributed across multiple partitions, which can be processed in parallel across a cluster.

Three words explain its characteristics:

- Resilient: Can recover lost partitions using lineage.
    
- Distributed: Data is divided into partitions across the cluster.
    
- Dataset: A collection of records or elements.
    

### RDD characteristics

|Characteristic|Meaning|
|---|---|
|Distributed|Data is divided into partitions|
|Immutable|Existing RDD cannot be modified; transformations create new RDDs|
|Lazy|Transformations are evaluated when an action is called|
|Fault-tolerant|Lost partitions can be recomputed using lineage|
|In-memory processing|Can cache data in memory for reuse|
|Parallel processing|Partitions can be processed concurrently|

## 2. Creating RDDs

There are two commonly used ways to create RDDs.

### A. `parallelize()` – Create RDD from a local collection

Use `parallelize()` when data is already available in your Python program.

```
data = [10, 20, 30, 40, 50]

rdd = sc.parallelize(data)

print(rdd.collect())
```

Output:

```
[10, 20, 30, 40, 50]
```

Specify the number of partitions:

```
rdd = sc.parallelize(data, 3)

print(rdd.getNumPartitions())
```

Output:

```
3
```

Use case: Small datasets, testing and experimentation.

### B. `textFile()` – Create RDD from a file

Use `textFile()` to read a text file into an RDD.

```
rdd = sc.textFile("file:///home/cloudera/data.txt")
```

Each line becomes one RDD element.

Suppose `data.txt` contains:

```
Apache Spark
Big Data Analytics
Hadoop
```

Then:

```
print(rdd.collect())
```

Output:

```
['Apache Spark', 'Big Data Analytics', 'Hadoop']
```

For HDFS:

```
rdd = sc.textFile("hdfs:///user/company/data.txt")
```

You can also specify the minimum number of partitions:

```
rdd = sc.textFile("hdfs:///user/company/data.txt", 4)
```

Important: The second argument is a minimum partition request, not an unconditional guarantee of exactly four partitions. Actual partitioning depends on the input files and Spark's input-splitting behavior.

### Comparison

|`parallelize()`|`textFile()`|
|---|---|
|Reads a local collection|Reads files|
|Data originates in the Python program|Data originates from a storage system|
|Useful for testing/small data|Useful for processing stored datasets|
|`sc.parallelize(data)`|`sc.textFile(path)`|

## 3. RDD Transformations

Transformations create a new RDD from an existing RDD. They are lazily evaluated.

### A. `map()` – Transform each element

Applies a function to every element.

```
rdd = sc.parallelize([1, 2, 3, 4])

result = rdd.map(lambda x: x * 2)

print(result.collect())
```

Output:

```
[2, 4, 6, 8]
```

### B. `flatMap()` – Transform and flatten

`flatMap()` can produce multiple output elements from one input element and flattens the resulting collections.

```
rdd = sc.parallelize(["Hello Spark", "Big Data"])

result = rdd.flatMap(lambda x: x.split(" "))

print(result.collect())
```

Output:

```
['Hello', 'Spark', 'Big', 'Data']
```

Difference:

```
map()                       flatMap()

["Hello Spark",             ["Hello Spark",
 "Big Data"]                 "Big Data"]
      |                           |
 split()                      split()
      |                           |
[["Hello","Spark"],          ["Hello","Spark"],
 ["Big","Data"]]              ["Big","Data"]]
      |                           |
 Nested collection           Flattened collection
```

|`map()`|`flatMap()`|
|---|---|
|One output per input element|Zero, one or multiple outputs per input|
|Preserves nested results|Flattens results by one level|
|Useful for element transformation|Useful for splitting records into individual elements|

### C. `filter()` – Filter elements

Retains elements satisfying a condition.

```
rdd = sc.parallelize([10, 15, 20, 25, 30])

result = rdd.filter(lambda x: x > 20)

print(result.collect())
```

Output:

```
[25, 30]
```

### D. `distinct()` – Remove duplicates

Returns an RDD containing distinct elements.

```
rdd = sc.parallelize([10, 20, 10, 30, 20, 40])

result = rdd.distinct()

print(result.collect())
```

Output (order may vary):

```
[10, 20, 30, 40]
```

`distinct()` generally involves a shuffle because duplicate values can exist in different partitions.

### E. `union()` – Combine two RDDs

Combines elements of two RDDs. It does not automatically remove duplicates.

```
rdd1 = sc.parallelize([1, 2, 3])
rdd2 = sc.parallelize([3, 4, 5])

result = rdd1.union(rdd2)

print(result.collect())
```

Output:

```
[1, 2, 3, 3, 4, 5]
```

To remove duplicates:

```
result = rdd1.union(rdd2).distinct()
```

### F. `intersection()` – Common elements

Returns elements present in both RDDs.

```
rdd1 = sc.parallelize([1, 2, 3, 4])
rdd2 = sc.parallelize([3, 4, 5, 6])

print(rdd1.intersection(rdd2).collect())
```

Output (order may vary):

```
[3, 4]
```

### G. `sortBy()` – Sort elements

```
rdd = sc.parallelize([50, 10, 40, 20, 30])

print(rdd.sortBy(lambda x: x).collect())
```

Output:

```
[10, 20, 30, 40, 50]
```

Descending order:

```
print(rdd.sortBy(lambda x: x, ascending=False).collect())
```

Output:

```
[50, 40, 30, 20, 10]
```

Sorting across the complete RDD generally requires a shuffle.

## 4. RDD Actions

Actions trigger execution and either return results to the Driver or write results to an external destination.

### A. `collect()` – Return all elements

```
rdd = sc.parallelize([10, 20, 30])

print(rdd.collect())
```

Output:

```
[10, 20, 30]
```

Important

Avoid `collect()` on large datasets because all elements are brought to the Driver, potentially causing an out-of-memory error.

### B. `count()` – Count elements

```
rdd = sc.parallelize([10, 20, 30, 40])

print(rdd.count())
```

Output:

```
4
```

### C. `first()` – Return first element

```
rdd = sc.parallelize([10, 20, 30])

print(rdd.first())
```

Output:

```
10
```

### D. `take(n)` – Return first n elements

```
rdd = sc.parallelize([10, 20, 30, 40, 50])

print(rdd.take(3))
```

Output:

```
[10, 20, 30]
```

### E. `top(n)` – Return largest n elements

```
rdd = sc.parallelize([10, 50, 20, 40, 30])

print(rdd.top(3))
```

Output:

```
[50, 40, 30]
```

By default, `top()` uses descending order.

### F. `reduce()` – Aggregate elements

Combines all elements using a function.

```
rdd = sc.parallelize([10, 20, 30, 40])

result = rdd.reduce(lambda a, b: a + b)

print(result)
```

Output:

```
100
```

Other example:

```
rdd.reduce(lambda a, b: a * b)
```

For `[1, 2, 3, 4]`, the result is `24`.

The function should be associative and commutative for predictable distributed aggregation.

### G. `foreach()` – Execute a function on each element

```
rdd = sc.parallelize([10, 20, 30])

rdd.foreach(lambda x: print(x))
```

The function executes on the workers. Its printed output may appear in Executor logs rather than your client terminal.

### H. `saveAsTextFile()` – Save RDD to storage

```
rdd = sc.parallelize(["Spark", "Hadoop", "Hive"])

rdd.saveAsTextFile("output")
```

Spark creates an output directory containing partition output files, typically named like:

```
output/
    part-00000
    part-00001
    _SUCCESS
```

The output directory must not already exist.

### Transformations vs Actions – Reference

|Transformations|Actions|
|---|---|
|`map()`|`collect()`|
|`flatMap()`|`count()`|
|`filter()`|`first()`|
|`distinct()`|`take()`|
|`union()`|`top()`|
|`intersection()`|`reduce()`|
|`sortBy()`|`foreach()`|
|`reduceByKey()`|`countByKey()`|
|`groupByKey()`|`saveAsTextFile()`|

## 5. Pair RDDs – Key-Value RDDs

A Pair RDD is an RDD whose elements are key-value pairs, generally represented as Python tuples.

Example:

```
data = [
    ("Mobile", 1000),
    ("Laptop", 2000),
    ("Mobile", 1500),
    ("Laptop", 3000),
    ("Tablet", 1200)
]

rdd = sc.parallelize(data)
```

Conceptually:

```
("Mobile", 1000)
("Laptop", 2000)
("Mobile", 1500)
("Laptop", 3000)
("Tablet", 1200)
```

Pair RDDs are useful for grouping, aggregation, counting and sorting by keys.

### A. `reduceByKey()` – Aggregate values for each key

Suppose we want total sales per product.

```
rdd = sc.parallelize([
    ("Mobile", 1000),
    ("Laptop", 2000),
    ("Mobile", 1500),
    ("Laptop", 3000),
    ("Tablet", 1200)
])

result = rdd.reduceByKey(lambda a, b: a + b)

print(result.collect())
```

Output (order may vary):

```
[
    ('Mobile', 2500),
    ('Laptop', 5000),
    ('Tablet', 1200)
]
```

How it works:

```
Input
  |
  |-- Mobile: 1000
  |-- Laptop: 2000
  |-- Mobile: 1500
  |-- Laptop: 3000
  |-- Tablet: 1200
  |
Group/aggregate by key
  |
  |-- Mobile: 1000 + 1500 = 2500
  |-- Laptop: 2000 + 3000 = 5000
  |-- Tablet: 1200
  |
Output
```

`reduceByKey()` performs local aggregation (map-side combining) before transferring data across partitions where possible. This can reduce shuffle data.

### B. `groupByKey()` – Group all values for each key

```
result = rdd.groupByKey()

print([(k, list(v)) for k, v in result.collect()])
```

Output (order may vary):

```
[
    ('Mobile', [1000, 1500]),
    ('Laptop', [2000, 3000]),
    ('Tablet', [1200])
]
```

Unlike `reduceByKey()`, it does not aggregate the values into one result. It groups them into an iterable.

### C. `reduceByKey()` vs `groupByKey()`

|`reduceByKey()`|`groupByKey()`|
|---|---|
|Aggregates values for each key|Groups values for each key|
|Produces one aggregated value per key|Produces an iterable of values per key|
|Supports local combining|Does not perform value aggregation|
|Usually more efficient for aggregation|Can transfer much more data during shuffle|

Exam rule: If the requirement is to calculate sum, count or maximum per key, prefer `reduceByKey()` or a suitable aggregate operation rather than grouping all values with `groupByKey()`.

### D. `sortByKey()` – Sort based on keys

```
rdd = sc.parallelize([
    ("C", 30),
    ("A", 10),
    ("B", 20)
])

print(rdd.sortByKey().collect())
```

Output:

```
[('A', 10), ('B', 20), ('C', 30)]
```

Descending:

```
print(rdd.sortByKey(ascending=False).collect())
```

### E. `countByKey()` – Count occurrences of each key

```
rdd = sc.parallelize([
    ("A", 10),
    ("B", 20),
    ("A", 30),
    ("C", 40),
    ("A", 50)
])

print(rdd.countByKey())
```

Output:

```
defaultdict(<class 'int'>, {'A': 3, 'B': 1, 'C': 1})
```

`countByKey()` is an action. Its result is returned to the Driver as a dictionary-like object. Use it only when the number of distinct keys is manageable.

### F. `countByValue()` – Count occurrences of each element

Unlike `countByKey()`, this works on the actual elements of an RDD.

```
rdd = sc.parallelize([10, 20, 10, 30, 20, 10])

print(rdd.countByValue())
```

Output:

```
{10: 3, 20: 2, 30: 1}
```

### Pair RDD method comparison

|Method|Purpose|Type|
|---|---|---|
|`reduceByKey()`|Aggregate values per key|Transformation|
|`groupByKey()`|Group values per key|Transformation|
|`sortByKey()`|Sort by key|Transformation|
|`countByKey()`|Count records per key|Action|
|`countByValue()`|Count occurrences of each element|Action|

## 6. Partition inspection and processing

RDDs are divided into partitions. These methods help inspect and control them.

### A. `getNumPartitions()`

Returns the number of partitions.

```
rdd = sc.parallelize([10, 20, 30, 40, 50], 3)

print(rdd.getNumPartitions())
```

Output:

```
3
```

### B. `glom()` – View elements partition-wise

`glom()` converts each partition into a list, allowing you to inspect how elements are distributed.

```
rdd = sc.parallelize([10, 20, 30, 40, 50, 60], 3)

print(rdd.glom().collect())
```

Conceptual output:

```
[[10, 20], [30, 40], [50, 60]]
```

Exact distribution can depend on partitioning.

### C. `mapPartitions()` – Process an entire partition

Unlike `map()`, which processes one element at a time, `mapPartitions()` applies a function to an iterator representing an entire partition.

```
def process_partition(iterator):
    values = list(iterator)
    yield sum(values)

rdd = sc.parallelize([1, 2, 3, 4, 5, 6], 3)

result = rdd.mapPartitions(process_partition)

print(result.collect())
```

Possible output:

```
[3, 7, 11]
```

Each partition produces one sum.

Use case: Operations where processing a group of records together is more efficient, such as initializing a database connection once per partition rather than once per record.

## 7. Caching and Persistence

Suppose you have an expensive transformation:

```
rdd = sc.textFile("hdfs:///data/sales.txt")

filtered = rdd.filter(lambda x: "Mobile" in x)
```

Now you perform multiple actions:

```
print(filtered.count())

print(filtered.take(10))
```

Without caching, Spark may recompute the transformation for each action.

### A. `cache()`

Caches the RDD so computed partitions can be reused.

```
filtered.cache()

print(filtered.count())
print(filtered.take(10))
```

The first action computes the RDD and populates the cache. Subsequent actions can reuse the cached partitions.

For RDDs, `cache()` uses the default `MEMORY_ONLY` storage level.

### B. `persist()`

`persist()` allows you to specify a storage level.

```
from pyspark import StorageLevel

filtered.persist(StorageLevel.MEMORY_AND_DISK)
```

Common RDD storage levels:

|Storage Level|Meaning|
|---|---|
|`MEMORY_ONLY`|Store in memory; recompute partitions that do not fit|
|`MEMORY_AND_DISK`|Store in memory; spill remaining partitions to disk|
|`DISK_ONLY`|Store partitions on disk|
|`MEMORY_ONLY_2`|Memory storage with two replicas|
|`MEMORY_AND_DISK_2`|Memory/disk storage with two replicas|

The availability and exact behavior of storage levels can depend on the Spark API/version.

### C. `unpersist()`

Removes the cached/persisted data when it is no longer needed.

```
filtered.unpersist()
```

### D. `cache()` vs `persist()`

|`cache()`|`persist()`|
|---|---|
|Uses default storage level|Allows storage level selection|
|Convenient|More control|
|RDD default: `MEMORY_ONLY`|Can use memory, disk and replication options|

Important: Caching is lazy. Calling `cache()` does not immediately calculate and store the entire RDD. An action is needed to materialize its partitions.

## 8. Word Count using RDDs – Complete example

This is a classic Spark program and demonstrates multiple RDD operations together.

Problem: Read a text file, split it into words and calculate the frequency of each word.

Suppose `input.txt` contains:

```
Spark is a big data engine
Spark processes big data
Hadoop is a big data framework
```

### Step 1: Read the file

```
lines = sc.textFile("input.txt")
```

### Step 2: Convert lines into words using `flatMap()`

```
words = lines.flatMap(lambda line: line.split())
```

Intermediate result:

```
["Spark", "is", "a", "big", "data", "engine",
 "Spark", "processes", "big", "data",
 "Hadoop", "is", "a", "big", "data", "framework"]
```

### Step 3: Convert each word into a key-value pair

```
pairs = words.map(lambda word: (word, 1))
```

Example:

```
("Spark", 1)
("is", 1)
("a", 1)
("big", 1)
("data", 1)
```

### Step 4: Aggregate using `reduceByKey()`

```
counts = pairs.reduceByKey(lambda a, b: a + b)
```

### Step 5: Display the result

```
print(counts.collect())
```

Output (order may vary):

```
[
    ('Spark', 2),
    ('is', 2),
    ('a', 2),
    ('big', 3),
    ('data', 3),
    ('engine', 1),
    ('processes', 1),
    ('Hadoop', 1),
    ('framework', 1)
]
```

### Complete program

```

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("WordCount")
    .master("local[*]")
    .getOrCreate()
)

sc = spark.sparkContext

# Read text file
lines = sc.textFile("input.txt")

# Split lines into individual words
words = lines.flatMap(lambda line: line.split())

# Convert words into (word, 1) pairs
pairs = words.map(lambda word: (word, 1))

# Count occurrences of each word
counts = pairs.reduceByKey(lambda a, b: a + b)

# Display results
for word, count in counts.collect():
    print(word, count)

spark.stop()
```

### Word Count with stopword removal

Now suppose we do not want common words such as `is`, `a`, `the`, `and` in our result.

These are called stopwords.

We can filter them before creating key-value pairs.

```
stopwords = {"is", "a", "the", "and"}

words = lines.flatMap(
    lambda line: line.lower().split()
)

filtered_words = words.filter(
    lambda word: word not in stopwords
)

pairs = filtered_words.map(lambda word: (word, 1))

counts = pairs.reduceByKey(lambda a, b: a + b)

print(counts.collect())
```

The processing pipeline is:

```
       Text File
           |
       textFile()
           |
          Lines
           |
       flatMap()
           |
          Words
           |
        filter()
      Remove stopwords
           |
       map(word, 1)
           |
      reduceByKey()
           |
       Word Counts
```

Note: This simple version does not remove punctuation. For example, `spark` and `spark,` would be counted as different words. Text normalization can be added before counting.