# Apache Flink Message Routing Solution

Route IBM MQ messages to different Kafka topics based on JMS properties using Apache Flink.

## Overview

This solution uses Apache Flink (SQL or Java) to consume messages from `ibm.mq.input` topic and route them to separate topics based on the `messageType` JMS property.

**Advantages:**
- ✅ Production-grade stream processing
- ✅ Exactly-once processing semantics
- ✅ High throughput and low latency
- ✅ Advanced windowing and aggregations
- ✅ Stateful processing capabilities
- ✅ Native support in Confluent Cloud

**Message Flow:**
```
IBM MQ → Connector → ibm.mq.input topic
                          ↓
                      Flink Job
                      /   |   \
                     /    |    \
            payment-topic | notification-topic
                    transfer-topic
```

## Deployment Options

### Option 1: Confluent Cloud for Apache Flink (Recommended)

**Advantages:**
- Fully managed
- Auto-scaling
- Integrated with Confluent Cloud Kafka
- No infrastructure management

**Steps:**
1. Go to Confluent Cloud → Flink (currently in preview/GA)
2. Create a Flink compute pool
3. Open Flink SQL workspace
4. Copy and paste statements from `routing.sql`
5. Submit as Flink job

**Configuration in Confluent Cloud:**
```properties
# Auto-configured by Confluent Cloud:
kafka.bootstrap.servers = <auto-configured>
security.protocol = SASL_SSL
sasl.mechanism = PLAIN
# API keys managed via Confluent Cloud
```

### Option 2: Self-Managed Flink

**Prerequisites:**
- Apache Flink 1.18+ cluster
- Kafka connector JAR (`flink-connector-kafka`)
- Access to Kafka cluster

**Steps:**

1. **Install Flink:**
```bash
wget https://dlcdn.apache.org/flink/flink-1.18.1/flink-1.18.1-bin-scala_2.12.tgz
tar xzf flink-1.18.1-bin-scala_2.12.tgz
cd flink-1.18.1
```

2. **Start Flink cluster:**
```bash
./bin/start-cluster.sh
```

3. **Start SQL Client:**
```bash
./bin/sql-client.sh
```

4. **Configure Kafka connection:**
```sql
SET 'kafka.bootstrap.servers' = 'your-kafka-bootstrap:9092';
SET 'security.protocol' = 'SASL_SSL';
SET 'sasl.mechanism' = 'PLAIN';
SET 'sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="<api-key>" password="<api-secret>";';
```

5. **Run SQL statements** from `routing.sql`

## Flink SQL Approach

See `routing.sql` for complete implementation.

### Key Features

**1. Nested Field Access:**
```sql
properties.messageType.`string`  -- Access nested JMS property
```

**2. Processing Time Attributes:**
```sql
`proc_time` AS PROCTIME()  -- For time-based operations
```

**3. Tumbling Windows:**
```sql
SELECT
  TUMBLE_START(proc_time, INTERVAL '1' MINUTE) AS window_start,
  properties.messageType.`string` AS messageType,
  COUNT(*) AS message_count
FROM ibm_mq_input
GROUP BY
  TUMBLE(proc_time, INTERVAL '1' MINUTE),
  properties.messageType.`string`;
```

**4. Primary Keys for Deduplication:**
```sql
PRIMARY KEY (`messageID`) NOT ENFORCED  -- Deduplication based on messageID
```

## Flink Table API (Java)

For programmatic control, use the Flink Table API. See `FlinkMessageRouter.java` for a complete example.

### Build and Run

```bash
cd flink-routing/java-router
mvn clean package

# Submit to Flink cluster
flink run -c io.confluent.flink.FlinkMessageRouter \
  target/flink-message-router-1.0.0.jar \
  --kafka.bootstrap.servers your-kafka:9092 \
  --kafka.api.key <api-key> \
  --kafka.api.secret <api-secret>
```

## Advanced Features

### 1. Exactly-Once Processing

Flink provides exactly-once semantics with checkpointing:

```sql
-- Enable checkpointing (set in Flink config or job properties)
SET 'execution.checkpointing.interval' = '60s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
```

### 2. Message Enrichment

Add computed fields or enrich with reference data:

```sql
INSERT INTO enriched_payments
SELECT
  messageID,
  `timestamp`,
  text,
  properties.messageType.`string` AS messageType,
  JSON_VALUE(text, '$.transactionId') AS transactionId,
  CAST(JSON_VALUE(text, '$.amount') AS DECIMAL(10,2)) AS amount,
  TO_TIMESTAMP(FROM_UNIXTIME(`timestamp` / 1000)) AS eventTime,
  CURRENT_TIMESTAMP AS processedTime
FROM ibm_mq_input
WHERE properties.messageType.`string` = 'PAYMENT';
```

### 3. Filtering and Validation

Route only valid messages:

```sql
INSERT INTO high_value_payments
SELECT
  messageID,
  `timestamp`,
  text,
  properties.messageType.`string` AS messageType
FROM ibm_mq_input
WHERE properties.messageType.`string` = 'PAYMENT'
  AND CAST(JSON_VALUE(text, '$.amount') AS DECIMAL(10,2)) > 10000
  AND JSON_VALUE(text, '$.transactionId') IS NOT NULL;
```

### 4. Aggregations and Windows

Count messages per minute:

```sql
CREATE TABLE message_counts_per_minute (
  `window_start` TIMESTAMP(3),
  `messageType` STRING,
  `message_count` BIGINT,
  PRIMARY KEY (`window_start`, `messageType`) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'message-counts',
  'properties.bootstrap.servers' = '${kafka.bootstrap.servers}',
  'format' = 'json'
);

INSERT INTO message_counts_per_minute
SELECT
  TUMBLE_START(proc_time, INTERVAL '1' MINUTE) AS window_start,
  properties.messageType.`string` AS messageType,
  COUNT(*) AS message_count
FROM ibm_mq_input
GROUP BY
  TUMBLE(proc_time, INTERVAL '1' MINUTE),
  properties.messageType.`string`;
```

### 5. Late Event Handling

Handle late-arriving messages with watermarks:

```sql
CREATE TABLE ibm_mq_input_with_watermark (
  -- ... other fields ...
  `timestamp` BIGINT,
  `event_time` AS TO_TIMESTAMP(FROM_UNIXTIME(`timestamp` / 1000)),
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'ibm.mq.input',
  'properties.bootstrap.servers' = '${kafka.bootstrap.servers}',
  'format' = 'json'
);
```

## Monitoring

### Confluent Cloud for Apache Flink

1. Navigate to Flink → Compute Pools
2. Select your compute pool
3. View jobs and metrics:
   - Records processed per second
   - Backpressure
   - Checkpointing stats
   - Task manager utilization

### Self-Managed Flink

Access Flink Web UI:
```
http://localhost:8081
```

Monitor:
- Job status and uptime
- Checkpointing success/failure
- Throughput and latency metrics
- Task parallelism and distribution

### Metrics via REST API

```bash
# Job overview
curl http://localhost:8081/jobs/overview

# Job metrics
curl http://localhost:8081/jobs/<job-id>/metrics

# Task manager metrics
curl http://localhost:8081/taskmanagers/metrics
```

## Performance Tuning

### Parallelism

Adjust parallelism for higher throughput:

```sql
-- Set default parallelism
SET 'parallelism.default' = '4';

-- Or set per-operator
CREATE TABLE payment_topic (
  -- ... fields ...
) WITH (
  'connector' = 'kafka',
  'topic' = 'payment-topic',
  'properties.bootstrap.servers' = '${kafka.bootstrap.servers}',
  'format' = 'json',
  'sink.parallelism' = '8'  -- Specific parallelism for this sink
);
```

### Checkpointing

```sql
SET 'execution.checkpointing.interval' = '60s';  -- More frequent for lower latency
SET 'execution.checkpointing.min-pause' = '30s';
SET 'state.backend' = 'rocksdb';  -- For large state
```

### Memory

```bash
# In flink-conf.yaml
taskmanager.memory.process.size: 2048m
taskmanager.memory.managed.fraction: 0.4
```

## Comparison: Flink SQL vs ksqlDB

| Feature | Flink SQL | ksqlDB |
|---------|-----------|--------|
| **Processing Semantics** | Exactly-once | At-least-once (default) |
| **Windowing** | Event time, processing time, session | Event time, session |
| **State Management** | RocksDB, heap | RocksDB |
| **Joins** | Stream-stream, stream-table, table-table | Stream-stream, stream-table |
| **Deployment** | Standalone, YARN, K8s, Confluent Cloud | Confluent Platform, Confluent Cloud |
| **SQL Standard** | ANSI SQL compliant | Custom SQL dialect |
| **Ecosystem** | Broader (batch + stream) | Kafka-focused |
| **Learning Curve** | Moderate | Easier |

**Choose Flink if:**
- You need exactly-once processing
- You have complex stateful operations
- You want event-time processing with watermarks
- You need batch + stream processing

**Choose ksqlDB if:**
- You're already in Confluent ecosystem
- You need quick time-to-value
- Your use case is primarily streaming
- You want simpler deployment

## Troubleshooting

### Job Fails to Start

Check Flink logs:
```bash
tail -f log/flink-*-taskexecutor-*.log
```

Common issues:
- Kafka connector JAR missing
- Incorrect Kafka credentials
- Insufficient resources

### No Data Flowing

```sql
-- Check source table
SELECT * FROM ibm_mq_input LIMIT 10;

-- Verify Kafka topic has data
-- In Flink SQL Client:
SELECT * FROM ibm_mq_input /*+ OPTIONS('scan.startup.mode'='earliest-offset') */ LIMIT 10;
```

### High Backpressure

- Increase parallelism
- Scale up task managers
- Check sink throughput (Kafka partition count)
- Review complex operations in the job

## Cleanup

```sql
-- Stop all jobs first (via Flink UI or CLI)

-- Drop tables
DROP TABLE IF EXISTS payment_topic;
DROP TABLE IF EXISTS transfer_topic;
DROP TABLE IF EXISTS notification_topic;
DROP TABLE IF EXISTS unknown_message_type_topic;
DROP TABLE IF EXISTS ibm_mq_input;
```

## Cost Optimization (Confluent Cloud)

Confluent Cloud for Apache Flink charges based on CFU (Confluent Flink Units):

- **Right-size compute pools**: Start small, scale up as needed
- **Use auto-pause**: Automatically pause when idle
- **Batch similar operations**: Combine filters/transformations
- **Monitor CFU usage**: Track via Confluent Cloud billing dashboard

## Next Steps

1. **Add Schema Registry**: Use Avro for schema evolution
2. **Implement alerting**: Send anomaly alerts to external systems
3. **Add metrics**: Export to Prometheus/Grafana
4. **Implement backfill**: Process historical data with batch mode
5. **Add testing**: Use Flink's testing framework for validation
