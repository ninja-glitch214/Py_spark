# Apache Kafka – Command Shortnotes

A practical CDAC BDA lab reference covering Kafka setup, topics, producers, consumers, consumer groups, offsets, partitions, configuration, and troubleshooting.

Kafka commands are executed in the Linux terminal, not inside the Hive or HBase shell.

The examples use the modern Kafka command-line tools. Depending on your installed Kafka version, scripts may be named `kafka-topics.sh` or `kafka-topics`; use the one available in your environment.

## 1. Start Kafka environment

Kafka requires a running Kafka broker. Depending on the Kafka version and configuration, it may use either ZooKeeper or KRaft mode.

Check Java:

```
java -version
```

Check Kafka installation:

```
kafka-topics.sh --version
```

ZooKeeper-based setup (legacy):

```
zookeeper-server-start.sh config/zookeeper.properties
```

Start Kafka broker in another terminal:

```
kafka-server-start.sh config/server.properties
```

KRaft-based setup (modern):

KRaft does not require ZooKeeper. The cluster must first be formatted using a cluster ID and the configured storage directory.

```
kafka-storage.sh random-uuid
```

Format storage using the generated ID:

```
kafka-storage.sh format \
  -t <CLUSTER_ID> \
  -c config/server.properties
```

Start Kafka:

```
kafka-server-start.sh config/server.properties
```

The formatting step is normally performed only when initializing a new KRaft storage directory, not every time Kafka starts.

## 2. Basic Kafka commands

| Command                        | Purpose                            |
| ------------------------------ | ---------------------------------- |
| `kafka-topics.sh`              | Topic management                   |
| `kafka-console-producer.sh`    | Send messages                      |
| `kafka-console-consumer.sh`    | Read messages                      |
| `kafka-consumer-groups.sh`     | Manage and inspect consumer groups |
| `kafka-configs.sh`             | View and modify configurations     |
| `kafka-broker-api-versions.sh` | Check broker API versions          |
| `kafka-metadata-quorum.sh`     | Inspect KRaft quorum metadata      |
| `kafka-storage.sh`             | Manage Kafka storage formatting    |

For commands that communicate with a broker, specify:

```
--bootstrap-server localhost:9092
```

This identifies the Kafka broker address used to establish the connection.

## 3. Topic management commands

A topic is a logical category or stream in which Kafka stores messages.

### Create a topic

```
kafka-topics.sh --create \
  --topic students \
  --bootstrap-server localhost:9092
```

Create with partitions and replication factor:

```
kafka-topics.sh --create \
  --topic students \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

- `--topic`: Topic name.
- `--partitions`: Number of partitions.
- `--replication-factor`: Number of copies of each partition.

Important: Replication factor cannot exceed the number of available brokers.

### List topics

```
kafka-topics.sh --list \
  --bootstrap-server localhost:9092
```

### Describe a topic

```
kafka-topics.sh --describe \
  --topic students \
  --bootstrap-server localhost:9092
```

Displays partition count, replication factor, leaders, replicas, and in-sync replicas.

### Delete a topic

```
kafka-topics.sh --delete \
  --topic students \
  --bootstrap-server localhost:9092
```

Topic deletion must be enabled in the broker configuration, and deletion may take time to complete.

### Check whether a topic exists

```
kafka-topics.sh --list \
  --bootstrap-server localhost:9092
```

### Increase partition count

```
kafka-topics.sh --alter \
  --topic students \
  --partitions 5 \
  --bootstrap-server localhost:9092
```

Important: Kafka allows increasing the number of partitions, but not decreasing them through this command. Increasing partitions can change key-based partition mapping for future messages.

## 4. Producer commands – Send messages

A producer sends messages to a Kafka topic.

### Start console producer

```
kafka-console-producer.sh \
  --topic students \
  --bootstrap-server localhost:9092
```

Type messages and press Enter:

```
Alice
Bob
Charlie
```

Each line is sent as a separate message.

### Send messages with keys

```
kafka-console-producer.sh \
  --topic students \
  --bootstrap-server localhost:9092 \
  --property parse.key=true \
  --property key.separator=:
```

Enter:

```
101:Alice
102:Bob
103:Charlie
```

Here:

- Before `:` is the message key.
- After `:` is the message value.

Messages with the same key are normally routed to the same partition when using the default key-based partitioner.

### Send messages from a file

```
kafka-console-producer.sh \
  --topic students \
  --bootstrap-server localhost:9092 \
  < students.txt
```

Each line of the file is sent as a separate message.

## 5. Consumer commands – Read messages

A consumer reads messages from a Kafka topic.

### Read messages

```
kafka-console-consumer.sh \
  --topic students \
  --bootstrap-server localhost:9092
```

By default, the consumer reads messages according to its offset position. A newly created consumer without a committed offset generally uses the configured `auto.offset.reset` behaviour.

### Read from the beginning

```
kafka-console-consumer.sh \
  --topic students \
  --bootstrap-server localhost:9092 \
  --from-beginning
```

Reads available retained messages from the earliest available offset.

### Display message keys and values

```
kafka-console-consumer.sh \
  --topic students \
  --bootstrap-server localhost:9092 \
  --property print.key=true \
  --property key.separator=:
```

### Display keys, values, partitions and offsets

```
kafka-console-consumer.sh \
  --topic students \
  --bootstrap-server localhost:9092 \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true
```

### Read only a limited number of messages

```
kafka-console-consumer.sh \
  --topic students \
  --bootstrap-server localhost:9092 \
  --max-messages 5
```

### Read messages from a specific partition

```
kafka-console-consumer.sh \
  --topic students \
  --bootstrap-server localhost:9092 \
  --partition 0 \
  --offset earliest
```

Read from a particular offset:

```
kafka-console-consumer.sh \
  --topic students \
  --bootstrap-server localhost:9092 \
  --partition 0 \
  --offset 5
```

### Read from the latest messages

```
kafka-console-consumer.sh \
  --topic students \
  --bootstrap-server localhost:9092 \
  --offset latest \
  --partition 0
```

`--offset` is used with an explicitly assigned partition. `earliest` and `latest` are special positions, not literal numeric offsets.

Important: A consumer assigned a specific partition using `--partition` does not use normal consumer-group partition assignment.

## 6. Consumer groups

A consumer group allows multiple consumers to cooperate in consuming a topic.

### Start consumer with a group ID

```
kafka-console-consumer.sh \
  --topic students \
  --bootstrap-server localhost:9092 \
  --group student-group
```

### List consumer groups

```
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --list
```

### Describe a consumer group

```
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group student-group
```

Displays:

- Topic and partition.
- Current committed offset.
- Log end offset.
- Consumer lag.
- Consumer ID and host, when available.

### Delete a consumer group

```
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --delete \
  --group student-group
```

The group generally must be inactive for deletion.

### Reset consumer group offsets

First preview the reset:

```
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group student-group \
  --topic students \
  --reset-offsets \
  --to-earliest \
  --dry-run
```

Execute the reset:

```
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group student-group \
  --topic students \
  --reset-offsets \
  --to-earliest \
  --execute
```

Other reset options:

| Option           | Purpose                                      |
| ---------------- | -------------------------------------------- |
| `--to-earliest`  | Move offsets to earliest available position. |
| `--to-latest`    | Move offsets to latest position.             |
| `--to-offset 10` | Move to a specified offset.                  |
| `--shift-by 5`   | Shift current offsets forward by 5.          |
| `--shift-by -5`  | Shift current offsets backward by 5.         |
| `--to-datetime`  | Reset based on a date/time.                  |
| `--to-current`   | Reset to current committed position.         |

Important: Offset reset normally requires the consumer group to be inactive. Use `--dry-run` before `--execute`.

## 7. Topic partitions and offsets

A topic is divided into partitions. Each partition is an ordered, append-only sequence of records.

Example:

```
Topic: students

Partition 0 → Offset 0, 1, 2, 3
Partition 1 → Offset 0, 1, 2
Partition 2 → Offset 0, 1, 2, 3, 4
```

Important points:

- Ordering is guaranteed within a partition, not across the entire topic.
- Each partition has its own offset sequence.
- A consumer group can have at most one active consumer assigned to a particular partition at a time.
- Different consumer groups independently maintain their offsets.
- Messages are retained according to retention policies, not deleted immediately after consumption.

### Inspect topic partitions

```
kafka-topics.sh --describe \
  --topic students \
  --bootstrap-server localhost:9092
```

### Read a specific partition from a particular offset

```
kafka-console-consumer.sh \
  --topic students \
  --bootstrap-server localhost:9092 \
  --partition 1 \
  --offset 2
```

## 8. Topic configuration and retention

Kafka stores messages for a configured retention period or until a configured storage limit is reached.

### Describe topic configuration

```
kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name students \
  --describe
```

### Set retention time

Set retention to 1 day:

```
kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name students \
  --alter \
  --add-config retention.ms=86400000
```

### Set maximum topic size

```
kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name students \
  --alter \
  --add-config retention.bytes=1073741824
```

### Set cleanup policy

```
kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name students \
  --alter \
  --add-config cleanup.policy=compact
```

Cleanup policies:

| Policy           | Meaning                                                |
| ---------------- | ------------------------------------------------------ |
| `delete`         | Deletes old log segments according to retention rules. |
| `compact`        | Retains the latest value for each key over time.       |
| `compact,delete` | Enables both compaction and time/size-based deletion.  |

### Remove a topic-level configuration override

```
kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name students \
  --alter \
  --delete-config retention.ms
```

Remember: Kafka's default cleanup policy is normally `delete`. Compaction is useful for retaining the latest state associated with each key, such as a customer's latest profile.

## 9. Replication and broker information

Replication maintains copies of partitions across brokers for fault tolerance.

### Describe replication details

```
kafka-topics.sh --describe \
  --topic students \
  --bootstrap-server localhost:9092
```

Example output:

```
Topic: students
Partition: 0
Leader: 1
Replicas: 1,2,3
Isr: 1,2,3
```

| Term     | Meaning                                                            |
| -------- | ------------------------------------------------------------------ |
| Leader   | Broker handling reads and writes for that partition.               |
| Replica  | Copy of a partition maintained by a broker.                        |
| ISR      | In-Sync Replicas; replicas sufficiently caught up with the leader. |
| Follower | Replica that follows the partition leader.                         |

Important: A replication factor of 3 means three copies of each partition, not three partitions.

## 10. Kafka configuration commands

### Display broker configuration

```
kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type brokers \
  --entity-name 1 \
  --describe
```

### Display topic configuration

```
kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name students \
  --describe
```

### Change topic configuration

```
kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name students \
  --alter \
  --add-config retention.ms=3600000
```

### Important broker properties

| Property              | Purpose                                                   |
| --------------------- | --------------------------------------------------------- |
| `broker.id`           | Unique broker identifier in ZooKeeper-based clusters.     |
| `node.id`             | Node identifier in KRaft configurations.                  |
| `listeners`           | Addresses on which the broker listens.                    |
| `log.dirs`            | Directory for Kafka log data.                             |
| `num.partitions`      | Default partition count for automatically created topics. |
| `log.retention.hours` | Default log retention duration in hours.                  |
| `zookeeper.connect`   | ZooKeeper connection configuration in legacy mode.        |
| `process.roles`       | Defines KRaft node roles.                                 |

These are generally configured in `server.properties`. Not all properties apply to both ZooKeeper and KRaft deployments.

## 11. Kafka utility commands

### Check broker API versions

```
kafka-broker-api-versions.sh \
  --bootstrap-server localhost:9092
```

### Check cluster metadata quorum (KRaft)

```
kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 \
  describe --status
```

### Generate a random KRaft cluster ID

```
kafka-storage.sh random-uuid
```

### Check storage information (KRaft)

```
kafka-storage.sh info \
  -c config/server.properties
```

## 12. Useful producer options

| Option                                      | Purpose                                            |
| ------------------------------------------- | -------------------------------------------------- |
| `--topic`                                   | Destination topic.                                 |
| `--bootstrap-server`                        | Kafka broker address.                              |
| `--property parse.key=true`                 | Parse message keys.                                |
| `--property key.separator=:`                | Separate key and value.                            |
| `--producer-property acks=all`              | Require acknowledgement from all in-sync replicas. |
| `--producer-property compression.type=gzip` | Compress messages using GZIP.                      |

Example:

```
kafka-console-producer.sh \
  --topic students \
  --bootstrap-server localhost:9092 \
  --producer-property acks=all
```

## 13. Useful consumer options

| Option                            | Purpose                                                                            |
| --------------------------------- | ---------------------------------------------------------------------------------- |
| `--topic`                         | Topic to consume.                                                                  |
| `--bootstrap-server`              | Broker address.                                                                    |
| `--group`                         | Consumer group ID.                                                                 |
| `--from-beginning`                | Read from earliest available offset when no valid committed position is available. |
| `--partition`                     | Explicitly assign a partition.                                                     |
| `--offset`                        | Starting position for explicitly assigned partition.                               |
| `--max-messages`                  | Stop after consuming a specified number of messages.                               |
| `--timeout-ms`                    | Exit after a period without receiving messages.                                    |
| `--property print.key=true`       | Display message keys.                                                              |
| `--property print.partition=true` | Display partition numbers.                                                         |
| `--property print.offset=true`    | Display offsets.                                                                   |

Example:

```
kafka-console-consumer.sh \
  --topic students \
  --bootstrap-server localhost:9092 \
  --group student-group \
  --from-beginning \
  --max-messages 10
```

## 14. Docker-based Kafka commands

If Kafka is running inside a Docker container, execute Kafka commands inside that container.

Enter the container:

```
docker exec -it kafka-container bash
```

Start Kafka command:

```
kafka-topics.sh --list \
  --bootstrap-server localhost:9092
```

Alternatively, execute directly without entering the container:

```
docker exec -it kafka-container \
  kafka-topics.sh --list \
  --bootstrap-server localhost:9092
```

Check running containers:

```
docker ps
```

View Kafka logs:

```
docker logs kafka-container
```

Follow logs continuously:

```
docker logs -f kafka-container
```

Replace `kafka-container` with your actual container name. The broker address may be different when connecting from outside the container or from another container.

## 15. Complete practical workflow

Step 1 – Create a topic

```
kafka-topics.sh --create \
  --topic students \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

Step 2 – Verify topic

```
kafka-topics.sh --list \
  --bootstrap-server localhost:9092

kafka-topics.sh --describe \
  --topic students \
  --bootstrap-server localhost:9092
```

Step 3 – Start producer

```
kafka-console-producer.sh \
  --topic students \
  --bootstrap-server localhost:9092
```

Enter messages:

```
Alice
Bob
Charlie
```

Step 4 – Start consumer in another terminal

```
kafka-console-consumer.sh \
  --topic students \
  --bootstrap-server localhost:9092 \
  --from-beginning
```

Step 5 – Start consumer with group ID

```
kafka-console-consumer.sh \
  --topic students \
  --bootstrap-server localhost:9092 \
  --group student-group
```

Step 6 – Inspect consumer group

```
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group student-group
```

Step 7 – Delete topic

```
kafka-topics.sh --delete \
  --topic students \
  --bootstrap-server localhost:9092
```

## 16. Quick revision – Important command families

| Task                     | Commands                                                  |
| ------------------------ | --------------------------------------------------------- |
| Start Kafka              | `kafka-server-start.sh`                                   |
| Start ZooKeeper (legacy) | `zookeeper-server-start.sh`                               |
| Topic management         | `--create`, `--list`, `--describe`, `--delete`, `--alter` |
| Producer                 | `kafka-console-producer.sh`                               |
| Consumer                 | `kafka-console-consumer.sh`                               |
| Consumer groups          | `--list`, `--describe`, `--delete`, `--reset-offsets`     |
| Configuration            | `kafka-configs.sh`                                        |
| Broker information       | `kafka-broker-api-versions.sh`                            |
| KRaft management         | `kafka-storage.sh`, `kafka-metadata-quorum.sh`            |

## 17. Important CDAC exam distinctions

| Concept            | Remember                                                                   |
| ------------------ | -------------------------------------------------------------------------- |
| Topic              | Logical stream/category of messages.                                       |
| Partition          | Divides a topic into independently ordered logs.                           |
| Offset             | Position of a record within a partition.                                   |
| Producer           | Publishes messages.                                                        |
| Consumer           | Reads messages.                                                            |
| Broker             | Kafka server that stores and serves partitions.                            |
| Consumer group     | Group of consumers sharing topic consumption.                              |
| Leader             | Handles reads and writes for a partition.                                  |
| Follower           | Replicates the leader's data.                                              |
| ISR                | In-Sync Replicas.                                                          |
| Replication factor | Number of copies of each partition.                                        |
| Consumer lag       | Difference between the log end position and consumer's committed position. |
| ZooKeeper          | Legacy Kafka cluster coordination mechanism.                               |
| KRaft              | Kafka's built-in metadata and coordination mechanism.                      |
| Retention          | Determines how long or how much data Kafka keeps.                          |
| Compaction         | Retains the latest record for each key over time.                          |

Five practical points to remember:

1. Kafka messages are not automatically deleted after a consumer reads them.
2. Ordering is guaranteed within a partition, not across all partitions of a topic.
3. Within one consumer group, a partition is assigned to at most one active consumer at a time.
4. Different consumer groups can independently read the same topic.
5. A consumer's offset tracks its reading progress; it is not the same thing as deleting a message.

This covers the commonly used Kafka commands for CDAC BDA practicals, including topic creation, message production/consumption, consumer groups, offsets, and basic administration.
