

# Part 2 – Spark Architecture and Internal Working



## 1. First, understand the big picture

Imagine an e-commerce company has a sales dataset containing 500 GB of data stored in HDFS.

The company wants to calculate total sales for each product.

A PySpark program might look conceptually like this:

```
sales = spark.read.csv("hdfs:///data/sales.csv")

result = sales.groupBy("product").sum("amount")

result.show()
```

You wrote only three operations:

1. Read the data.
    
2. Group records by product and calculate the sum.
    
3. Display the result.
    

But Spark has to coordinate several components to execute this program.

![Spark Architecture: Driver, Executors, Cluster Managers - Euron Daily | Euron Community](https://images.openai.com/static-rsc-4/PDPlRzEpWr4adPbCcr55eFK3LXLG9JzlU0s8Z9aRjVY_inkj1D0siCQkJKI6e_RBZdnuXRjNx3mUooo0N982n6yNA5ZGBMg4gOJFPJ2lmO2TAT5pYBtjol7HTwBm1SJAZ2fLqvo6yOuE5vTlW75MxkuekGPqaRdg50EFX5SV5_o?purpose=inline)

[euron.one](https://euron.one/community/posts/19ae332c-a088-4c6c-b0cb-8ebbe8f18534?utm_source=chatgpt.com)

The important components are:

|Component|Responsibility|
|---|---|
|Driver|Coordinates the Spark application|
|Executor|Executes tasks and stores cached data|
|Worker Node|Machine that hosts executors|
|Cluster Manager|Allocates resources across the cluster|

Let's understand them through a story.

## 2. Driver, Executors and Worker Nodes

Imagine Spark is a company handling a large data-processing project.

### A. Driver – The project manager

The Driver is the central coordinator of a Spark application.

It is responsible for:

- Running the main application logic.
    
- Creating the execution plan.
    
- Dividing work into jobs, stages and tasks.
    
- Scheduling tasks on executors.
    
- Collecting results and coordinating execution.
    

Think of the Driver as the manager who understands the complete project and distributes work.

Important: The Driver coordinates the execution; it does not normally perform all the distributed data processing itself.

### B. Executor – The workers doing the actual processing

An Executor is a process launched for a Spark application on a worker machine.

Its responsibilities include:

- Executing tasks assigned by the Driver.
    
- Processing data partitions.
    
- Storing cached or persisted data.
    
- Sending task status and results back to the Driver.
    

For example, suppose Spark has divided a dataset into four partitions:

![[Pasted image 20261110191603.png|716]]

The Driver schedules the tasks, and the Executors execute them.

### C. Worker Node – The machine hosting executors

A Worker Node is a machine in the cluster that provides resources such as CPU and memory for running executors.

Do not confuse these terms:

- Worker Node = machine.
    
- Executor = process running on that machine.
    
- Task = unit of work executed by an executor.
    

One Worker Node can host multiple Executors, depending on the cluster configuration and resources.

Also, a Driver can run on a machine that is not one of the worker nodes.

### D. How the components relate

```
             Spark Application
                    |
                 Driver
                    |
          Requests cluster resources
                    |
              Cluster Manager
                    |
          Allocates resources on nodes
                    |
          -----------------------
          |                     |
      Worker Node 1         Worker Node 2
          |                     |
      Executor 1             Executor 2
       /     \                /     \
    Task 1  Task 2         Task 3  Task 4
```

This is the basic Spark architecture.

Remember the distinction: The Driver plans and coordinates; the Cluster Manager allocates resources; Executors execute tasks; Worker Nodes provide the machines on which Executors run.

## 3. Cluster Manager – Who provides the resources?

We now know that the Driver needs Executors to process data.

But who decides which machines have enough CPU and memory to run those Executors?

That is the job of the Cluster Manager.

A Cluster Manager manages cluster resources and helps Spark obtain the resources needed by an application.

Spark supports multiple cluster managers.

### A. Spark Standalone

Spark has its own built-in cluster manager called Standalone.

It is relatively simple and is designed specifically to manage Spark applications.

```
Spark Standalone Cluster

        Master
          |
    -------------
    |           |
 Worker 1    Worker 2
    |           |
 Executor    Executor
```

- Master manages the cluster's resources.
    
- Workers provide resources.
    
- Executors run on workers.
    

Do not confuse the Standalone Master with the Spark Driver. They perform different roles.

### B. Hadoop YARN

YARN (Yet Another Resource Negotiator) is Hadoop's resource-management layer.

It can allocate resources to different applications, including Spark.

For example, the same Hadoop cluster might run:

- Spark applications.
    
- MapReduce applications.
    
- Other YARN-supported applications.
    

```
             YARN Resource Manager
                      |
             Node Managers
                /       \
             Node 1    Node 2
               |         |
           Container  Container
               |         |
           Executor   Executor
```

In YARN:

- ResourceManager = overall resource management.
    
- NodeManager = manages resources and containers on an individual node.
    
- Container = allocated bundle of resources in which an application component can run.
    

A Spark Driver may run inside a YARN ApplicationMaster in cluster deployment mode. The ApplicationMaster also coordinates with YARN to request resources.

### C. Kubernetes

Kubernetes is a container orchestration platform.

Spark can use Kubernetes to launch and manage Driver and Executor containers (typically as pods).

It is especially relevant in containerized and cloud-native environments.

### D. Comparison

|Cluster Manager|Main purpose|
|---|---|
|Standalone|Spark's built-in resource manager|
|YARN|Hadoop ecosystem resource manager|
|Kubernetes|Container orchestration and resource management|
|Mesos|Historical/general cluster manager; support is deprecated in modern Spark releases|

Exam trap: HDFS is not a Cluster Manager. HDFS stores distributed data; YARN manages cluster resources.

## 4. Client Mode vs Cluster Mode

This is one of the most important Spark architecture concepts.

We already know that a Spark application has a Driver and Executors.

But where does the Driver actually run?

Spark supports two common deployment modes:

- Client Mode
    
- Cluster Mode
    

Let's understand them with an example.

Suppose you submit a Spark application from your laptop to a remote cluster.

### A. Client Mode

In Client Mode, the Driver runs on the machine from which you submit the application.

```
        Your Laptop
        (Client Machine)
              |
            Driver
              |
        Cluster Manager
          /       \
      Worker 1   Worker 2
         |          |
     Executor  Executor
```

The Driver stays on your laptop, while Executors run on the cluster's worker machines.

Advantages:

- Convenient for interactive development.
    
- You can directly see Driver logs and output in your terminal.
    
- Useful for debugging.
    

Disadvantage:

- If your laptop disconnects or the Driver process terminates, the application can fail.

### B. Cluster Mode

In Cluster Mode, the Driver runs inside the cluster, rather than on the client machine.

```
       Your Laptop
       (Client Machine)
              |
        Submit Application
              |
        Cluster Manager
              |
       -----------------
       |               |
    Worker 1        Worker 2
       |               |
     Driver         Executor
       |
     Executor
```

The exact placement depends on the cluster manager's allocation.

Advantages:

- The application is independent of the client machine after submission.
    
- Suitable for production and long-running applications.
    
- The Driver runs closer to cluster resources.
    

### C. Quick comparison

|Feature|Client Mode|Cluster Mode|
|---|---|---|
|Driver location|Client machine|Cluster|
|Executors|Cluster|Cluster|
|Client disconnects|Can terminate the application if Driver is lost|Application can continue|
|Typical usage|Interactive development|Production jobs|

Important exam point

Client Mode and Cluster Mode describe the location of the Driver, not where the data is stored or where Executors run.

Both modes can use distributed Executors.

## 5. SparkSession and SparkContext

Now let's move from cluster architecture to the programming interface.

When you write a PySpark application, how do you actually start interacting with Spark?

You generally begin by creating a SparkSession.

### A. SparkSession

SparkSession is the main entry point for working with Spark's higher-level APIs, especially:

- DataFrames
    
- Spark SQL
    
- Reading and writing data
    
- Creating DataFrames
    
- Accessing the underlying SparkContext
    

Example:

```
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("SalesAnalysis")
    .master("local[*]")
    .getOrCreate()
)
```

Let's break it down:

|Expression|Meaning|
|---|---|
|`SparkSession.builder`|Starts configuring a session|
|`.appName("SalesAnalysis")`|Sets the application name|
|`.master("local[*]")`|Runs locally using available logical cores|
|`.getOrCreate()`|Gets an existing session or creates one|

A few important `.master()` examples:

|Value|Meaning|
|---|---|
|`local`|Local execution using one thread|
|`local[2]`|Local execution using two threads|
|`local[*]`|Local execution using available logical cores|
|`spark://host:7077`|Connects to a Spark Standalone cluster|
|`yarn`|Uses YARN as the cluster manager|
|`k8s://...`|Uses Kubernetes|

`local[*]` does not mean that Spark uses all machines in a cluster. It uses available logical cores on the local machine.

### B. SparkContext

SparkContext is the lower-level entry point for Spark Core functionality, especially RDD operations and cluster interaction.

You can access it through SparkSession:

```
sc = spark.sparkContext

print(sc.version)
print(sc.master)
print(sc.appName)
```

Conceptually:

```
SparkSession
     |
     |-- DataFrame API
     |
     |-- Spark SQL
     |
     |-- SparkContext
              |
              |-- RDD API
              |
              |-- Core Spark functionality
```

Modern PySpark applications generally use SparkSession as their primary entry point.

Exam distinction:

- SparkSession → primary entry point for DataFrame and SQL operations.
    
- SparkContext → lower-level entry point for RDD and core functionality.
    

## 6. How PySpark communicates with the JVM

This is where the architecture becomes particularly interesting.

You write Python code, but Spark's core execution engine is primarily implemented in Scala and runs on the JVM (Java Virtual Machine).

So how does Python communicate with it?

### A. The basic architecture

PySpark provides a Python API that communicates with the Spark engine.

Conceptually:

```
       Your Python Program
                |
             PySpark
          Python API layer
                |
        Communication bridge
                |
          Spark Engine
        (Scala / JVM)
                |
        -----------------
        |               |
     Executor        Executor
       JVM              JVM
```

The Python API lets you write Spark applications using Python, while the underlying Spark engine performs much of the distributed planning and execution.

### B. Does every Python operation run on the JVM?

No. This distinction is important.

There are different execution paths:

1. DataFrame operations: Spark SQL's engine plans and executes many operations on the JVM. Python is mainly used to define the operations.
    
2. RDD operations with Python functions: Python functions such as `map()` can execute in Python worker processes associated with Executors.
    
3. Python UDFs: User-defined Python functions generally require data to cross between JVM and Python processes, which introduces serialization and communication overhead.
    

For example:

```
df = spark.read.csv("sales.csv")

result = df.groupBy("_c0").count()

result.show()
```

The DataFrame operations are planned and executed using Spark's SQL engine.

But consider:

```
rdd = sc.parallelize([1, 2, 3, 4])

result = rdd.map(lambda x: x * 2)

print(result.collect())
```

The Python lambda is executed by Python worker processes rather than simply becoming a native JVM operation.

This is one reason built-in Spark SQL functions are often preferable to Python UDFs for DataFrame processing.

Remember: PySpark is not a separate Python-based distributed engine. It is the Python interface to Apache Spark.

## 7. Lazy Evaluation – Why Spark does not execute immediately

This is one of the most fundamental ideas in Spark.

Imagine you write:

```
df = spark.read.csv("sales.csv")

filtered = df.filter(df.amount > 1000)

grouped = filtered.groupBy("product").count()
```

At this point, has Spark necessarily read the entire CSV, filtered the rows and grouped them?

No.

Spark uses a concept called Lazy Evaluation.

It means Spark generally does not execute transformations immediately. Instead, it records the operations and builds a plan for execution.

### A. Transformations

A transformation creates a new RDD or DataFrame from an existing one.

Examples:

- `map()`
    
- `filter()`
    
- `flatMap()`
    
- `select()`
    
- `withColumn()`
    
- `groupBy()`
    

For example:

```
df2 = df.filter(df.amount > 1000)
```

This defines what should happen, but normally does not trigger execution by itself.

### B. Actions

An action requests a result or triggers execution.

Examples:

|RDD actions|DataFrame actions|
|---|---|
|`collect()`|`show()`|
|`count()`|`count()`|
|`first()`|`collect()`|
|`take()`|`first()`|
|`reduce()`|`write()`|

For example:

```
df = spark.read.csv("sales.csv")

filtered = df.filter(df.amount > 1000)

grouped = filtered.groupBy("product").count()

grouped.show()
```

The `show()` action triggers the execution of the required computation.

### C. The execution flow

```
Transformation 1
       |
Transformation 2
       |
Transformation 3
       |
       |  No execution yet
       |
      Action
       |
 Spark plans computation
       |
 Spark executes computation
       |
    Result
```

### D. Why is lazy evaluation useful?

Suppose Spark sees:

```
df.filter(df.amount > 1000) \
  .select("product", "amount") \
  .groupBy("product") \
  .count() \
  .show()
```

Because Spark has the entire chain before execution, it can optimize the computation.

For example, it may avoid reading columns that are not required.

This is called query optimization in the DataFrame/Spark SQL execution path.

Lazy evaluation also allows Spark to combine operations into a more efficient execution plan.

Important distinction

Lazy evaluation does not mean Spark never executes until the program ends. An action triggers the required computation. A later action can trigger another computation unless previously computed data is cached or otherwise reusable.

## 8. DAG – Directed Acyclic Graph

Now that we understand lazy evaluation, let's understand what Spark builds before execution.

Spark builds a logical representation of the operations called a DAG (Directed Acyclic Graph).

Break the name into three parts:

- Directed → operations have a direction of dependency.
    
- Acyclic → no circular dependency.
    
- Graph → connected nodes representing operations.
    

### A. Example

Suppose we want to:

1. Read sales data.
    
2. Filter sales above ₹1,000.
    
3. Select product and amount.
    
4. Group by product.
    
5. Calculate total amount.
    

Conceptually:

```
       Read Sales Data
              |
        Filter > 1000
              |
      Select product, amount
              |
       Group By product
              |
        Sum(amount)
              |
             Show
```

This represents the dependency between operations.

Spark uses this information to decide how to execute the application.

### B. Is the DAG the same as a Job?

No. This is a common exam trap.

A DAG represents the dependency structure of computations. A Job is a unit of execution triggered by an action.

For example:

```
df.filter(df.amount > 1000).show()

df.groupBy("product").count().show()
```

These two actions can trigger two separate Spark jobs.

Each job has its own execution plan and stages, although Spark may reuse previously computed data if it has been cached.

### C. DAG Scheduler

The DAG Scheduler examines the computation and divides a job into stages based on dependencies, especially shuffle boundaries.

It then submits stages to the Task Scheduler.

The Task Scheduler is responsible for scheduling individual tasks on available Executors.

```
                 Action
                   |
               Spark Job
                   |
              DAG Scheduler
                   |
          Divides job into stages
                   |
              Task Scheduler
                   |
          Schedules tasks on executors
```

## 9. Jobs, Stages and Tasks – The execution hierarchy

This is extremely important for Spark MCQs.

Think of the hierarchy like this:

Spark Application

Complete running Spark application

Job

Triggered by an action

Stages

Separated by shuffle dependencies

Tasks

One task processes one partition in a stage

Let's understand each level.

### A. Application

A Spark Application is the complete application submitted to Spark.

It generally consists of:

- A Driver.
    
- Executors allocated for that application.
    
- One or more jobs.
    

### B. Job

A Job is triggered when an action requires Spark to compute a result.

Example:

```
df.count()
```

The `count()` action triggers a job.

### C. Stage

A Job is divided into one or more stages.

A stage is a collection of tasks that can execute without requiring a shuffle boundary between those tasks.

Spark commonly separates stages at wide transformations, which require data to be redistributed between partitions.

We will understand this in detail shortly.

### D. Task

A Task is the smallest unit of execution in this hierarchy.

A task processes one partition of data for a particular stage.

For example, suppose a stage has four partitions:

```
Stage 1
  |
  |-- Task 1 → Partition 1
  |-- Task 2 → Partition 2
  |-- Task 3 → Partition 3
  |-- Task 4 → Partition 4
```

Therefore, four tasks can potentially execute in parallel, subject to available resources.

Very important: The number of tasks in a stage is generally determined by the number of partitions being processed by that stage—not simply by the number of Executors.

## 10. Narrow vs Wide Transformations

Now we reach one of the most important concepts behind Spark's execution efficiency.

Remember:

- A transformation creates another RDD/DataFrame.
    
- A stage contains tasks that can run without crossing a shuffle boundary.
    
- Some transformations need data to move between partitions; others do not.
    

This leads to two categories.

### A. Narrow Transformation

A narrow transformation is one where each output partition depends on a limited number of input partitions—typically one.

Example: `filter()`.

Suppose we have four partitions:

```
Input Partitions       Output Partitions

     P1  ----------------> P1
     P2  ----------------> P2
     P3  ----------------> P3
     P4  ----------------> P4
```

Each output partition can be computed from its corresponding input partition.

There is no need to redistribute data across the cluster.

Examples:

- `map()`
    
- `filter()`
    
- `flatMap()`
    
- `mapPartitions()`
    
- `union()` (typically narrow dependencies)
    

### B. Wide Transformation

A wide transformation is one where an output partition may depend on data from multiple input partitions.

Example: `groupByKey()`.

Suppose the same key exists in different partitions:

```
P1: (A, 10)  (B, 20)
P2: (A, 30)  (C, 40)
P3: (B, 50)  (A, 60)
```

If we group records by key, all records with key `A` must be brought together.

```
             Input
          /    |    \
        P1     P2     P3
         \      |     /
          \     |    /
            Shuffle
                |
        ----------------
        |      |       |
      Key A  Key B   Key C
```

This requires data redistribution across partitions. That process is called Shuffle.

Examples of wide transformations include:

- `groupByKey()`
    
- `reduceByKey()`
    
- `distinct()`
    
- `repartition()`
    
- `sortByKey()`
    
- DataFrame `groupBy()`
    
- Many join operations
    

A few operations can have more nuanced dependency behavior depending on their implementation and partitioning. The important thing is to understand the data dependency, not memorize method names blindly.

### C. Comparison

|Narrow|Wide|
|---|---|
|Limited input partition dependency|Output may depend on multiple input partitions|
|No shuffle required between input and output partitions|Usually requires shuffle|
|Usually more efficient|Often more expensive|
|Can remain in the same stage|Creates a stage boundary|

## 11. Shuffle – Why does Spark need it?

Let's explore the previous example more deeply.

Suppose an e-commerce company has sales data:

|Partition|Product|Sales|
|---|---|---|
|P1|Mobile|100|
|P1|Laptop|200|
|P2|Mobile|300|
|P2|Tablet|150|
|P3|Laptop|400|
|P3|Mobile|250|

We want total sales per product.

```
df.groupBy("product").sum("sales")
```

Spark cannot calculate the final total for `Mobile` by looking at only one partition because its records are distributed across P1, P2 and P3.

It must bring the relevant records together.

Conceptually:

```
P1                  P2                  P3
Mobile: 100         Mobile: 300         Laptop: 400
Laptop: 200         Tablet: 150         Mobile: 250
    \                   |                   /
     \                  |                  /
      \_________________|_________________/
                        |
                      SHUFFLE
                        |
             ----------------------
             |          |         |
           Mobile     Laptop    Tablet
           650         600       150
```

### What happens during a shuffle?

At a high level:

1. Each input partition processes its local records.
    
2. Records are organized according to the required partitioning key.
    
3. Intermediate data is written and transferred as necessary.
    
4. Data is read by the destination partitions.
    
5. The next stage processes the redistributed data.
    

The shuffle may involve network communication, serialization, disk I/O and memory management.

That makes shuffle expensive compared with transformations that can operate locally on existing partitions.

### Why is shuffle important?

It is required for operations such as:

- Grouping by keys.
    
- Aggregations across partitions.
    
- Many joins.
    
- Global sorting.
    
- Repartitioning.
    

Exam trap: Shuffle is not the same as moving the entire dataset to the Driver. Data is redistributed between Executors/partitions; it does not mean that everything is collected at the Driver.

## 12. Partitions – How Spark divides data for parallel processing

We have repeatedly used the word partition. Let's understand it properly.

A Partition is a logical division of a distributed dataset.

Spark divides data into partitions so that different parts can be processed in parallel.

For example, imagine a 100 GB dataset divided into four partitions:

```
             100 GB Dataset
                   |
          -------------------
          |    |     |      |
         P1    P2    P3     P4
         25GB  25GB  25GB   25GB
```

Each partition can be processed by a separate task.

### A. Relationship between partitions and tasks

This is an important relationship:

One task processes one partition in a stage.

Suppose a stage processes eight partitions.

```
             Stage 1
                |
       ---------------------
       |   |   |   |   |   |
       T1  T2  T3  T4 T5  T6
       T7  T8
```

There are eight tasks in that stage.

However, suppose the cluster has only two available execution slots. Spark may execute only two tasks concurrently, then schedule the remaining tasks as resources become available.

Therefore:

- Partitions determine the amount of parallel work.
    
- Available CPU cores and resources determine how much of that work can execute concurrently.
    

### B. Are partitions the same as HDFS blocks?

No. This is a common confusion.

|HDFS Block|Spark Partition|
|---|---|
|Storage concept|Processing concept|
|Divides files into storage blocks|Divides data into processing units|
|Managed by HDFS|Managed by Spark|
|Example: 128 MB block|Partition size depends on input and Spark processing|

When Spark reads a file from HDFS, input partitions are often related to the file's block layout, but they are not guaranteed to be identical to HDFS blocks.

The number and size of partitions can also change during processing.

### C. Why do we need partitions?

- Enable parallel execution.
    
- Distribute workload across Executors.
    
- Control the degree of parallelism.
    
- Improve resource utilization.
    

Too few partitions can underutilize the cluster. Too many tiny partitions can introduce scheduling and task overhead.

We will revisit partition tuning in Part 7.

## 13. Lineage and Fault Tolerance

Now consider a situation where an Executor crashes while processing data.

Does Spark always need to restart the entire application?

Not necessarily.

One of Spark's important features is lineage-based fault tolerance, particularly for RDDs.

### A. What is lineage?

Lineage is the record of transformations used to create a dataset.

Suppose we write:

```
rdd1 = sc.textFile("hdfs:///data/sales.txt")

rdd2 = rdd1.filter(lambda x: "Mobile" in x)

rdd3 = rdd2.map(lambda x: x.split(","))
```

The relationship is:

```
Original Data
      |
  textFile()
      |
    rdd1
      |
   filter()
      |
    rdd2
      |
    map()
      |
    rdd3
```

Spark knows how `rdd3` was derived from the original data.

This transformation history is its lineage.

### B. How does lineage help?

Imagine one partition of `rdd3` is lost because an Executor fails.

Spark can use the lineage information to recompute the missing partition from its parent data and transformations.

Conceptually:

```
Original Data
      |
   filter()
      |
     map()
      |
  Partition P3 lost
      |
 Recompute required partition
      |
  Recovered P3
```

It does not necessarily need to recompute every partition of the dataset.

### C. Does lineage mean Spark never stores data?

No.

Spark can also cache or persist datasets to avoid repeatedly recomputing expensive operations.

For example:

```
rdd = sc.textFile("hdfs:///data/sales.txt")

filtered = rdd.filter(lambda x: "Mobile" in x)

filtered.cache()

print(filtered.count())
print(filtered.take(5))
```

The first action computes the data and allows cached partitions to be reused by later operations.

If a cached partition is lost, Spark can generally recompute it using lineage, provided the source data and required dependencies are still available.

### D. Lineage vs Caching

|Lineage|Caching|
|---|---|
|Tracks how data was created|Stores computed data for reuse|
|Helps recompute lost partitions|Avoids repeated computation|
|Part of Spark's fault-tolerance model|Performance optimization|
|Does not itself store the complete result|Uses configured storage levels|

Remember: Lineage helps recover lost data; caching helps reuse already-computed data.

## 14. Put the entire execution story together

Let's return to our original sales-analysis example.

```
df = spark.read.csv("hdfs:///data/sales.csv")

filtered = df.filter(df.amount > 1000)

result = filtered.groupBy("product").sum("amount")

result.show()
```

What happens internally?

1. Application starts
    
    The Driver starts and establishes communication with the cluster.
    
2. Resources are allocated
    
    The Cluster Manager allocates resources and Executors are launched on Worker Nodes.
    
3. Transformations are recorded
    
    Spark records the filter and aggregation operations. It does not immediately execute every transformation.
    
4. Action triggers a Job
    
    `show()` requests the result and triggers execution.
    
5. DAG Scheduler creates Stages
    
    Spark divides the computation at shuffle boundaries.
    
6. Task Scheduler schedules Tasks
    
    Tasks are assigned to Executors, with each task processing a partition.
    
7. Shuffle redistributes data
    
    Records are brought together according to their product key for aggregation.
    
8. Result is produced
    
    The final result is returned for display.
    

The whole picture:

```
                PYSPARK PROGRAM
                       |
                   Driver
                       |
                Cluster Manager
                       |
                 Executors
                       |
              Transformations
                       |
                Lazy Evaluation
                       |
                  Action
                       |
                 Spark Job
                       |
                DAG Scheduler
                       |
                  Stages
                       |
               Task Scheduler
                       |
                   Tasks
                       |
                 Partitions
                       |
          Shuffle where required
                       |
                  Results
```

This is the mental model you should carry into the next parts of Spark.

## 15. Exam-focused revision sheet

These are the distinctions I would make sure you can answer without hesitation.

|Concept|Remember this|
|---|---|
|Driver|Coordinates the Spark application|
|Executor|Executes tasks and can cache data|
|Worker Node|Machine hosting Executors|
|Cluster Manager|Allocates cluster resources|
|SparkSession|Main entry point for DataFrame and SQL APIs|
|SparkContext|Lower-level entry point for RDD/Core APIs|
|Transformation|Defines a computation|
|Action|Triggers computation|
|DAG|Dependency graph of computations|
|Job|Execution triggered by an action|
|Stage|Group of tasks separated by shuffle boundaries|
|Task|Processes one partition in a stage|
|Partition|Logical division of data|
|Shuffle|Redistributes data between partitions|
|Lineage|Transformation history used for recomputation|
|Cache|Retains computed data for reuse|

### Common CDAC-style traps

1. Driver vs Cluster Manager: The Driver coordinates an application; the Cluster Manager allocates resources.
    
2. Executor vs Worker Node: An Executor is a process, not a machine.
    
3. Job vs Stage: An action triggers a job; a job can contain multiple stages.
    
4. Stage vs Task: A stage contains tasks; each task processes a partition.
    
5. Transformation vs Action: `filter()` is a transformation; `count()` is an action.
    
6. Narrow vs Wide: Wide dependencies generally require shuffle.
    
7. Lineage vs Cache: Lineage enables recomputation; caching enables reuse.
    
8. Client vs Cluster Mode: The distinction is the Driver's location.
    
9. HDFS Block vs Spark Partition: One is for storage, the other for processing.
    
10. Shuffle vs Collect: Shuffle redistributes data between partitions; `collect()` brings results to the Driver.
    

## 16. Check your understanding

Try these questions before looking at the answer key.

Part 2 checkpoint

8/8 answered

Q1. Which component is responsible for allocating resources to a Spark application?

A. Driver

B. Executor

C. Cluster Manager

D. DAG Scheduler

Correct

The Cluster Manager allocates resources. The Driver coordinates the application after obtaining resources.

Q2. A Spark application has 10 partitions in a stage. How many tasks are generally created for that stage?

A. 1

B. 10

C. Depends only on executor count

D. 100

Correct

Generally, one task is created per partition for each stage.

Q3. Which operation is an action?

A. filter()

B. map()

C. withColumn()

D. count()

Correct

count() requests a result and triggers execution. The others are transformations.

Q4. Which operation typically introduces a shuffle?

A. filter()

B. map()

C. groupByKey()

D. flatMap()

Correct

groupByKey() must bring values for the same key together, requiring redistribution.

Q5. In Client Mode, where does the Driver run?

A. On the client machine

B. Always on the NameNode

C. On every Executor

D. Inside every Worker Node

Correct

In Client Mode, the Driver runs on the machine that submitted the application.

Q6. What is the main purpose of lineage?

A. Allocate CPU

B. Track transformations for recomputation

C. Store every result permanently

D. Replace the Cluster Manager

Correct

Lineage records dependencies so Spark can recompute lost partitions.

Q7. Which statement about SparkSession is correct?

A. It is the HDFS NameNode

B. It is the primary entry point for DataFrame and SQL APIs

C. It allocates YARN containers directly

D. It is an Executor

Correct

SparkSession is the main entry point for modern DataFrame and SQL-based PySpark applications.

Q8. Which statement about shuffle is correct?

A. It always sends all data to the Driver

B. It redistributes data between partitions

C. It is the same as caching

D. It happens only when reading CSV

Correct

Shuffle redistributes data between partitions, often involving network and disk I/O.

# 8 / 8

