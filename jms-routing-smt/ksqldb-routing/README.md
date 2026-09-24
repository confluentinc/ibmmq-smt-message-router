# ksqlDB Message Routing Solution

Route IBM MQ messages to different Kafka topics based on JMS properties using ksqlDB.

## Overview

This solution uses ksqlDB to consume messages from `ibm.mq.input` topic and route them to separate topics based on the `messageType` JMS property.

**Advantages:**
- ✅ No custom code required
- ✅ Works immediately in Confluent Cloud
- ✅ Real-time processing
- ✅ Easy to modify routing rules
- ✅ Built-in monitoring and metrics

**Message Flow:**
```
IBM MQ → Connector → ibm.mq.input topic
                          ↓
                       ksqlDB
                      /   |   \
                     /    |    \
            payment-topic | notification-topic
                    transfer-topic
```

## Prerequisites

1. IBM MQ Source Connector running and writing to `ibm.mq.input` topic
2. ksqlDB cluster (available in Confluent Cloud or self-managed)
3. Messages with JMS property `messageType` set

## Quick Start

### 1. Connect to ksqlDB

**Confluent Cloud:**
```bash
confluent ksql app list
confluent ksql app configure-acls <ksql-cluster-id> ibm.mq.input
```

Or use the Confluent Cloud UI: Environments → ksqlDB → Query

**Self-Managed:**
```bash
ksql http://localhost:8088
```

### 2. Run the SQL Statements

Copy and paste the statements from `routing.sql` into the ksqlDB CLI or UI.

### 3. Verify

```sql
-- Show created streams
SHOW STREAMS;

-- Watch payment messages in real-time
SELECT * FROM payment_topic EMIT CHANGES LIMIT 10;

-- Count messages by type
SELECT
  properties->messageType->string AS messageType,
  COUNT(*) AS message_count
FROM ibm_mq_input_stream
GROUP BY properties->messageType->string
EMIT CHANGES;
```

## Output Topics

After running the SQL:

| Topic | Contains | Partition Count |
|-------|----------|-----------------|
| `payment-topic` | Messages where messageType = 'PAYMENT' | 3 |
| `transfer-topic` | Messages where messageType = 'TRANSFER' | 3 |
| `notification-topic` | Messages where messageType = 'NOTIFICATION' | 3 |
| `unknown-message-type-topic` | Messages with missing or unknown messageType | 3 |

## Customization

### Add New Message Types

```sql
CREATE STREAM new_message_type_topic WITH (
  KAFKA_TOPIC='new-message-type-topic',
  VALUE_FORMAT='JSON'
) AS
SELECT
  messageID,
  timestamp,
  text,
  properties->messageType->string AS messageType
FROM ibm_mq_input_stream
WHERE properties->messageType->string = 'NEW_TYPE'
EMIT CHANGES;
```

### Extract Only the Payload

If you only want the JSON payload (not the full JMS message):

```sql
CREATE STREAM payment_topic WITH (
  KAFKA_TOPIC='payment-topic',
  VALUE_FORMAT='JSON'
) AS
SELECT
  text  -- Only the message body
FROM ibm_mq_input_stream
WHERE properties->messageType->string = 'PAYMENT'
EMIT CHANGES;
```

### Add Filtering Logic

Route only high-value payments:

```sql
CREATE STREAM high_value_payments WITH (
  KAFKA_TOPIC='high-value-payments',
  VALUE_FORMAT='JSON'
) AS
SELECT *
FROM ibm_mq_input_stream
WHERE properties->messageType->string = 'PAYMENT'
  AND CAST(EXTRACTJSONFIELD(text, '$.amount') AS BIGINT) > 10000
EMIT CHANGES;
```

### Add Transformations

Enrich messages with additional fields:

```sql
CREATE STREAM enriched_payments WITH (
  KAFKA_TOPIC='enriched-payments',
  VALUE_FORMAT='JSON'
) AS
SELECT
  messageID,
  timestamp,
  text,
  properties->messageType->string AS messageType,
  EXTRACTJSONFIELD(text, '$.transactionId') AS transactionId,
  CAST(EXTRACTJSONFIELD(text, '$.amount') AS BIGINT) AS amount,
  FROM_UNIXTIME(timestamp) AS processedTime
FROM ibm_mq_input_stream
WHERE properties->messageType->string = 'PAYMENT'
EMIT CHANGES;
```

## Monitoring

### View Query Status

```sql
SHOW QUERIES;
EXPLAIN <query-id>;
```

### Check Processing Rate

```sql
DESCRIBE EXTENDED payment_topic;
```

### Confluent Cloud Metrics

In Confluent Cloud UI:
- ksqlDB → Queries tab → View metrics
- Monitor messages/sec, consumer lag, errors

## Troubleshooting

### Stream not receiving messages

```sql
-- Check if source stream is receiving data
SELECT * FROM ibm_mq_input_stream EMIT CHANGES LIMIT 5;

-- Verify topic exists and has data
PRINT 'ibm.mq.input' FROM BEGINNING LIMIT 5;
```

### Wrong message type values

```sql
-- See all distinct message types
SELECT DISTINCT properties->messageType->string AS messageType
FROM ibm_mq_input_stream
EMIT CHANGES;
```

### Null messageType

If `messageType` is null, messages will go to `unknown_message_type_topic`. Check that JMS messages have the property set.

## Cleanup

To remove streams and queries:

```sql
DROP STREAM IF EXISTS payment_topic DELETE TOPIC;
DROP STREAM IF EXISTS transfer_topic DELETE TOPIC;
DROP STREAM IF EXISTS notification_topic DELETE TOPIC;
DROP STREAM IF EXISTS unknown_message_type_topic DELETE TOPIC;
DROP STREAM IF EXISTS ibm_mq_input_stream;
```

**Note:** `DELETE TOPIC` will delete the underlying Kafka topic. Omit if you want to keep the data.

## Performance Considerations

- **Partitions**: Adjust partition count based on throughput (default: 3)
- **Replicas**: Set based on availability requirements (default: 3)
- **Consumer Groups**: Each stream query runs in its own consumer group
- **Scaling**: ksqlDB automatically scales query processing

## Cost Optimization (Confluent Cloud)

ksqlDB in Confluent Cloud charges based on CSU (Confluent Streaming Units):
- Each persistent query uses resources
- Consider combining queries if possible
- Use `EMIT CHANGES` only when needed for streaming queries
- For one-time transformations, consider pull queries or batch processing

## Next Steps

1. **Add Schema Registry**: Use Avro/Protobuf for better schema management
2. **Add windowing**: Aggregate messages over time windows
3. **Join with other streams**: Enrich with reference data
4. **Add alerting**: Use Confluent Cloud connectors to send alerts for specific conditions
