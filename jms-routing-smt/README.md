# JMS Message Routing - Detailed Guide

Route JMS messages to different Kafka topics based on message properties.

**Tested:** IBM MQ, ActiveMQ Classic, ActiveMQ Artemis

Confluent's built-in `ExtractTopic$Value` SMT supports `field.format=JSON_PATH`, so no custom code is needed. See [JSONPATH_DISCOVERY.md](../JSONPATH_DISCOVERY.md) for the test results.

## Problem

JMS properties are nested in the connector's JSON output:

```json
{
  "properties": {
    "messageType": {
      "string": "PAYMENT"
    }
  }
}
```

Most SMTs can't extract `properties.messageType.string` directly.

## Which to Use?

| | **Flink SQL** | **JSON_PATH SMT** |
|-|---------------|-------------------|
| Input topic retained (audit, replay) | Yes | No |
| Delivery | Exactly-once on Confluent Cloud | Connector-dependent |
| Latency | 1-5 sec | Sub-second |
| Setup | ~15 min | ~5 min |
| Extra infrastructure | Flink compute pool | None |
| Aggregations | Yes | No |

- Need an audit trail, replay, several routing strategies on one input, or aggregations? → **[Flink SQL](flink-routing/)**
- Want minimal storage and direct routing? → **JSON_PATH SMT**

## JSON_PATH SMT

Add to your connector config:

```json
{
  "transforms": "routeByJsonPath",
  "transforms.routeByJsonPath.type": "io.confluent.connect.transforms.ExtractTopic$Value",
  "transforms.routeByJsonPath.field": "$[\"properties\"][\"messageType\"][\"string\"]",
  "transforms.routeByJsonPath.field.format": "JSON_PATH",
  "transforms.routeByJsonPath.skip.missing.or.null": "true"
}
```

The SMT extracts `properties.messageType.string` and uses it as the topic name, so a `messageType` of `PAYMENT` goes to the topic `PAYMENT`. With `skip.missing.or.null=true`, records without the property stay on the connector's `kafka.topic`.

See the [main README](../README.md#configuration) for full connector configs.

## Flink

```
MQ → Connector → ibm.mq.input → Flink → payment-topic, transfer-topic, notification-topic
```

Run the SQL from [flink-routing/routing.sql](flink-routing/routing.sql). [Full guide →](flink-routing/)

## Testing

Send a JMS message with the property:

```java
TextMessage msg = session.createTextMessage("{\"transactionId\":\"PAY-001\"}");
msg.setStringProperty("messageType", "PAYMENT");
producer.send(msg);
```

Verify the message arrives in the `PAYMENT` topic.

## Repository layout

```
jms-routing-smt/
├── flink-routing/   # Flink SQL for Confluent Cloud
└── README.md        # This file
```
