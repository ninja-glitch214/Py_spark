Perfect. We'll completely skip installation and environment configuration. Since your instructor has provided a preconfigured OVA, we'll treat it as our ready-to-use Spark environment.

We'll focus on understanding Spark, learning how it works, and becoming comfortable using it as a data engineer. When we reach the practical stage, we'll use the tools already available inside your OVA.

# Apache Spark – Learning Roadmap

We'll follow the roadmap we discussed, with one important principle: understand the role of each component before learning its commands or code.

|Part|What we'll learn|
|---|---|
|1. Spark Introduction|What Spark is, why it exists, capabilities, use cases, and its place in the Big Data ecosystem|
|2. Spark Architecture|Driver, Executors, Cluster Manager, DAG, Jobs, Stages, Tasks, lazy evaluation, shuffle, fault tolerance|
|3. Starting and Using Spark|Spark shell, PySpark shell, SparkSession, Python scripts, notebooks, `spark-submit` — using the existing OVA|
|4. RDD|RDD fundamentals, partitions, transformations, actions, key-value RDDs, caching|
|5. DataFrames and Spark SQL|DataFrames, schemas, SQL queries, file formats, joins, aggregations, Catalyst optimizer|
|6. PySpark Data Engineering|Data cleaning, transformations, UDFs, window functions, partitioning, reading and writing data|
|7. Spark Integrations|HDFS, Hive, HBase, Kafka, databases, cloud storage|
|8. Spark Streaming|Batch vs streaming, Structured Streaming, Kafka integration, windows, checkpointing|
|9. Advanced Spark|MLlib, GraphX, optimization, monitoring, Spark UI, troubleshooting|
|10. Practical Projects|End-to-end data engineering pipelines using the technologies we've learned|

# Part 1 – Spark Introduction

Let's begin by connecting Spark to everything you've learned so far.

## 1. The Big Data story: Why did we need Spark?

Imagine you're working as a data engineer for an e-commerce company.

Every day, the company generates:

- Customer transaction records.
    
- Website clickstream data.
    
- Product browsing history.
    
- Payment records.
    
- Customer reviews.
    
- Application logs.
    

Over time, this data grows from gigabytes to terabytes or even petabytes.

You already know how Hadoop addresses two major problems:

1. HDFS: How do we store enormous amounts of data across multiple machines?
    
2. MapReduce: How do we process that distributed data?
    

So we have:

![[Pasted image 20261110161804.png|757]]

But there was a problem.

### The limitation of traditional MapReduce

Suppose your company wants to perform a multi-step analysis:

1. Read transaction data.
    
2. Remove invalid records.
    
3. Filter transactions from the last year.
    
4. Group transactions by customer.
    
5. Calculate total spending.
    
6. Identify the top 100 customers.
    

In traditional MapReduce, these operations may require multiple MapReduce jobs.

A simplified workflow could look like this:

![[Pasted image 20261110161845.png|874]]

The important issue is that intermediate results are repeatedly written to and read from disk.

This introduces disk I/O overhead, especially when a workflow involves many stages or repeatedly uses the same dataset.

This is where Apache Spark enters the picture.

## 2. What exactly is Apache Spark?

A distributed data processing and analytics engine.

Apache Spark is designed to process large datasets across multiple machines.

Its key advantage is that it can keep intermediate data in memory when useful, rather than always writing every intermediate result to disk.

For example, our e-commerce workflow can be represented as a sequence of operations:

![[Pasted image 20261110161926.png|723]]

Instead of forcing every operation to become a separate MapReduce job, Spark can optimize a sequence of operations and execute them together.

One important clarification: Spark is not an entirely memory-only engine. It can still use disk for intermediate data, shuffle operations, caching overflow, and output. Its advantage is that it can avoid unnecessary disk I/O in suitable workloads.

## 3. Spark vs Hadoop MapReduce

|Feature|MapReduce|Spark|
|---|---|---|
|Processing model|Map and Reduce jobs|General distributed computation|
|Intermediate data|Commonly written to disk|Can be kept in memory|
|Multi-step workflows|Often require multiple jobs|Can optimize a sequence of operations|
|Processing speed|Disk I/O can add overhead|Often faster for iterative workloads|
|APIs|Primarily MapReduce programming model|RDD, DataFrame, SQL, etc.|
|Use cases|Batch processing|Batch, SQL, streaming, ML, etc.|

Remember: Spark did not make Hadoop obsolete. Spark and Hadoop can work together.

For example, Spark can process data stored in HDFS.

## 4. Where does Spark fit into the Big Data ecosystem?

This is especially important because you've already learned HDFS, MapReduce, Pig, Hive, HBase and Kafka.

Think of each technology as solving a different problem.

![[Pasted image 20261110162025.png|803]]


Notice that Spark is positioned as the processing engine, not as the storage system.

Let's connect the technologies individually:

|Technology|Its responsibility|How Spark works with it|
|---|---|---|
|HDFS|Distributed file storage|Reads and writes distributed files|
|Hive|SQL tables and metadata/catalog|Reads and writes Hive tables|
|HBase|NoSQL data storage|Reads and writes records through connectors|
|Kafka|Distributed event streaming platform|Consumes and processes incoming events|
|MapReduce|Distributed batch processing|Spark can perform many similar workloads using its own execution engine|

One subtle but important distinction:

- Hive provides table metadata and SQL-related capabilities. The underlying table data can reside in HDFS or object storage.
    
- HBase is a database, not a file storage system like HDFS.
    
- Kafka transports and retains event streams; Spark processes those streams.
    
- Spark processes data but does not replace these storage and ingestion systems.
    

## 5. What can Spark actually do?

Spark is not limited to one type of processing. Its ecosystem has several major components.

![[Pasted image 20261110162138.png|782]]

For our data engineering journey, we'll focus most heavily on Spark Core, Spark SQL, DataFrames and Structured Streaming.

## 6. The mental model to retain

Think of the technologies this way:

```
                  BIG DATA PIPELINE
                         |
          +--------------+--------------+
          |              |              |
       INGESTION       STORAGE       PROCESSING
          |              |              |
        Kafka           HDFS           Spark
                         |
                        HBase
                         |
                        Hive
                    (metadata/SQL)
```

This is a conceptual grouping, not a mandatory execution sequence. For example, Spark can read directly from Kafka, HDFS, databases or cloud storage.

The central idea:

HDFS stores the data. Kafka transports events. Hive manages table metadata and SQL access. HBase stores records. Spark processes data.

## What comes next?

We've established why Spark exists and where it fits into the ecosystem. Now we need to understand how Spark actually performs distributed processing.

Part 2 – Spark Architecture and Internal Working will cover:

1. Driver program
    
2. Executors
    
3. Cluster Manager
    
4. SparkSession and SparkContext
    
5. Jobs, Stages and Tasks
    
6. DAG (Directed Acyclic Graph)
    
7. Lazy evaluation
    
8. Transformations vs Actions
    
9. Narrow vs Wide transformations
    
10. Shuffle and fault tolerance
    

This is the most important conceptual foundation before we start writing PySpark code. We'll connect its architecture directly to the MapReduce and YARN concepts you already know.