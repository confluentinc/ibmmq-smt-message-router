# Flink SQL Message Routing

Route JMS messages from IBM MQ, ActiveMQ Classic or ActiveMQ Artemis to different Kafka topics based on the `messageType` JMS property, using Flink SQL on Confluent Cloud.

The JMS source connector writes every message to one input topic (e.g. `ibm.mq.input`). Flink reads that topic and routes each message to its own topic. Messages stay in the input topic, so you keep an audit trail and can replay.

```
MQ → JMS Source Connector → ibm.mq.input (all messages retained)
                                   │
                                   ▼
                               Flink job (stateless filter per type)
                      ┌──────────┬─┴────────┬───────────────┐
                      ▼          ▼          ▼               ▼
               payment-topic  transfer-  notification-  unknown-message-
                              topic      topic          type-topic
```

Messages with a missing or unrecognised `messageType` go to `unknown-message-type-topic` instead of being dropped.

**Tested with:** IBM MQ 9.x, ActiveMQ Classic 6.3.2, ActiveMQ Artemis 2.40.0. All three produce the same nested structure, `properties.messageType.string`. End-to-end latency measured on Confluent Cloud: 1-5 seconds.

> Routing is a stateless filter: it uses no event time and no watermarks. The windowing examples further down use processing time.

## Files

| File | Use |
|------|-----|
| `routing.sql` | Confluent Cloud Flink, IBM MQ (`ibm.mq.input`) |
| `activemq-routing.sql` | Confluent Cloud Flink, ActiveMQ Classic (`activemq.input`) |
| `artemis-routing.sql` | Confluent Cloud Flink, ActiveMQ Artemis (`artemis.input`) |

## Run on Confluent Cloud for Apache Flink

Prerequisites: a JMS source connector writing to the input topic, and a Flink compute pool.

1. Prepare Schema Registry. Flink needs a JSON schema without `null` types. Either soft-delete the existing `<topic>-value` subject and let Flink create it, or make sure the schema has no `"type": "null"` fields.
2. Open the SQL workspace for your compute pool.
3. Run the statements in `routing.sql` (or the ActiveMQ/Artemis file), one at a time and in order: source table, sink tables, then the four `INSERT` statements.
4. Check the **Jobs** tab for four running jobs, send test messages, and watch the output topics.

Connectivity, security and Schema Registry are configured by Confluent Cloud.

## SQL Building Blocks

Nested field access:

```sql
properties.messageType.`string`
```

Count messages per minute, using a processing-time attribute (add `` `proc_time` AS PROCTIME() `` to the source table first):

```sql
SELECT
  TUMBLE_START(proc_time, INTERVAL '1' MINUTE) AS window_start,
  properties.messageType.`string` AS messageType,
  COUNT(*) AS message_count
FROM `ibm.mq.input`
GROUP BY
  TUMBLE(proc_time, INTERVAL '1' MINUTE),
  properties.messageType.`string`;
```

On Confluent Cloud, `PRIMARY KEY (messageID) NOT ENFORCED` on a sink makes `messageID` the Kafka record key. It does **not** deduplicate messages.

### Enrichment and filtering

```sql
INSERT INTO high_value_payments
SELECT
  messageID,
  `timestamp`,
  text,
  properties.messageType.`string` AS messageType,
  JSON_VALUE(text, '$.transactionId') AS transactionId,
  CAST(JSON_VALUE(text, '$.amount') AS DECIMAL(10,2)) AS amount
FROM `ibm.mq.input`
WHERE properties.messageType.`string` = 'PAYMENT'
  AND CAST(JSON_VALUE(text, '$.amount') AS DECIMAL(10,2)) > 10000
  AND JSON_VALUE(text, '$.transactionId') IS NOT NULL;
```

### Event time

The `timestamp` field is declared `DOUBLE`. Its unit depends on what produced the message (JMS timestamps are normally epoch milliseconds; the test sender in this repo's docs used epoch seconds). Check your messages before building event-time logic. For epoch milliseconds:

```sql
CREATE TABLE `ibm.mq.input` (
  messageID STRING,
  `timestamp` DOUBLE,
  properties ROW<messageType ROW<propertyType STRING, `string` STRING>>,
  text STRING,
  `event_time` AS TO_TIMESTAMP_LTZ(CAST(`timestamp` AS BIGINT), 3),
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
) WITH (
  'connector' = 'confluent',
  'value.format' = 'json-registry'
);
```

## Flink vs the SMT Alternatives

| | **Flink** | **JSON_PATH SMT** (built in) | **Custom SMT** (historical) |
|-|-----------|------------------------------|-----------------------------|
| Input topic retained | Yes | No | No |
| Delivery | Exactly-once on Confluent Cloud | Connector-dependent | Connector-dependent |
| Latency | 1-5 sec | Sub-second | Sub-second |
| Extra infrastructure | Flink compute pool | None | Custom connector plugin |
| Aggregations / joins | Yes | No | No |

Use Flink when you need the input topic for audit or replay, or any stateful processing. For simple routing, use the [JSON_PATH SMT](../README.md); it needs no extra infrastructure. The custom SMT is kept for reference only.

## Troubleshooting

**No data flowing**

```sql
SELECT * FROM `ibm.mq.input` LIMIT 10;
```

Check that the topic has data and that the source table schema matches it.

**Schema errors** (`NullSchema`, "Schema doesn't match"): delete the subject in Schema Registry and let Flink recreate it, or remove `"type": "null"` fields.

**Jobs running but no output**: check records in/out on the Jobs tab, and look for exceptions in the job details.

## Cleanup

Stop the jobs first, then drop the tables:

```sql
DROP TABLE IF EXISTS `payment-topic`;
DROP TABLE IF EXISTS `transfer-topic`;
DROP TABLE IF EXISTS `notification-topic`;
DROP TABLE IF EXISTS `unknown-message-type-topic`;
DROP TABLE IF EXISTS `ibm.mq.input`;
```

## Cost (Confluent Cloud)

Flink is billed in CFUs. Start with a small compute pool and scale up, and combine similar filters where you can.
