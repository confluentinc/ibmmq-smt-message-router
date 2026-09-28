# JMS Message Routing to Kafka

Route JMS messages to different Kafka topics based on message properties.

**Tested with:** IBM MQ, ActiveMQ Classic, ActiveMQ Artemis

## Problem

A single MQ queue contains different message types (payments, transfers, notifications). You want to route each type to its own Kafka topic.

JMS Source Connectors store properties in nested JSON:

```json
{
  "text": "{\"transactionId\":\"PAY-001\",\"amount\":5000}",
  "properties": {
    "messageType": {
      "string": "PAYMENT"
    }
  }
}
```

Standard SMTs can't extract from nested paths like `properties.messageType.string`.

## Solutions

Two approaches tested and working in Confluent Cloud:

### Option 1: Flink SQL

Route using Flink SQL. Messages land in an input topic first, then Flink routes them.

```sql
INSERT INTO payment_topic SELECT * FROM jms_input WHERE messageType = 'PAYMENT';
```

**Pros:** Exactly-once, keeps input topic, supports aggregations  
**Cons:** Needs Flink cluster, 1-5 second latency

[See Flink guide →](jms-routing-smt/flink-routing/)

### Option 2: JSON_PATH SMT

Route at connector level using Confluent's built-in SMT. Messages route directly, no input topic.

```json
{
  "transforms": "routeByJsonPath",
  "transforms.routeByJsonPath.type": "io.confluent.connect.transforms.ExtractTopic$Value",
  "transforms.routeByJsonPath.field": "$[\"properties\"][\"messageType\"][\"string\"]",
  "transforms.routeByJsonPath.field.format": "JSON_PATH",
  "transforms.routeByJsonPath.skip.missing.or.null": "true"
}
```

**Pros:** No extra infrastructure, sub-second, no custom code  
**Cons:** No input topic, no aggregations

### Comparison

| | **Flink** | **JSON_PATH SMT** |
|-|-----------|-------------------|
| **Input topic retained?** | Yes | No |
| **Exactly-once?** | Yes | Connector-dependent |
| **Latency** | 1-5 seconds | Sub-second |
| **Aggregations?** | Yes | No |
| **Setup time** | 15 minutes | 5 minutes |

## How It Works

**Flink:**
```
MQ → Connector → jms.input (all messages) → Flink → payment, transfer, notification topics
```

**JSON_PATH SMT:**
```
MQ → Connector with SMT → payment, transfer, notification topics (direct)
```

## Which to Use?

**Use Flink if:**
- Need exactly-once guarantees
- Want messages in input topic (audit, reprocessing)
- Need aggregations or windowing

**Use JSON_PATH SMT if:**
- Don't need input topic
- Want simple direct routing
- Prefer minimal infrastructure

## Prerequisites

**MQ Side:**
- Single queue with messages that have JMS properties (e.g., `messageType`)
- Any JMS provider: IBM MQ, ActiveMQ Classic, or ActiveMQ Artemis

**Kafka Side:**
- JMS Source Connector
- Flink compute pool (if using Flink)

See [Setting JMS Properties](#setting-jms-properties) for code examples.

## Quick Start

### Flink (15 minutes)

1. Open Flink SQL workspace in Confluent Cloud
2. Run SQL from [jms-routing-smt/flink-routing/routing.sql](jms-routing-smt/flink-routing/routing.sql)

[Full guide →](jms-routing-smt/flink-routing/)

### JSON_PATH SMT (5 minutes)

Add this to your connector config:

```json
{
  "transforms": "routeByJsonPath",
  "transforms.routeByJsonPath.type": "io.confluent.connect.transforms.ExtractTopic$Value",
  "transforms.routeByJsonPath.field": "$[\"properties\"][\"messageType\"][\"string\"]",
  "transforms.routeByJsonPath.field.format": "JSON_PATH",
  "transforms.routeByJsonPath.skip.missing.or.null": "true"
}
```

[See full configs →](#configuration)

## Tested With

- IBM MQ 9.x
- ActiveMQ Classic 6.3.2
- ActiveMQ Artemis 2.40.0

All three use the same nested property structure (`properties.messageType.string`), so configs work across all providers.

## Configuration

### JSON_PATH SMT

Add to your connector config (works for IBM MQ, ActiveMQ Classic, and ActiveMQ Artemis):

```json
{
  "connector.class": "IbmMQSource",  // or "ActiveMQSource"
  "output.data.format": "JSON",
  "transforms": "routeByJsonPath",
  "transforms.routeByJsonPath.type": "io.confluent.connect.transforms.ExtractTopic$Value",
  "transforms.routeByJsonPath.field": "$[\"properties\"][\"messageType\"][\"string\"]",
  "transforms.routeByJsonPath.field.format": "JSON_PATH",
  "transforms.routeByJsonPath.skip.missing.or.null": "true"
}
```

The SMT extracts `properties.messageType.string` from the message JSON and routes to that topic name.

For full connector configs, see [jms-routing-smt/README.md](jms-routing-smt/README.md).

## Setting JMS Properties

Set a JMS property on your messages:

```java
TextMessage msg = session.createTextMessage("{\"transactionId\":\"PAY-001\"}");
msg.setStringProperty("messageType", "PAYMENT");
producer.send(msg);
```

The connector converts this to nested JSON:
```json
{
  "properties": {
    "messageType": {
      "string": "PAYMENT"
    }
  }
}
```

Then the routing extracts `properties.messageType.string` → `"PAYMENT"` → routes to `PAYMENT` topic.

## Resources

- [IBM MQ Source Connector](https://docs.confluent.io/cloud/current/connectors/cc-ibmmq-source.html)
- [ActiveMQ Source Connector](https://docs.confluent.io/cloud/current/connectors/cc-activemq-source.html)
- [Flink on Confluent Cloud](https://docs.confluent.io/cloud/current/flink/overview.html)
- [ExtractTopic SMT](https://docs.confluent.io/cloud/current/connectors/transforms/extracttopic.html)
