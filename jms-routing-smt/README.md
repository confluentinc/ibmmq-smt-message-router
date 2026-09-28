# JMS Message Routing - Detailed Guide

**Discovery (Sept 2026):** Confluent's `ExtractTopic$Value` SMT supports JSON_PATH. No custom code needed.

Route JMS messages to different Kafka topics based on message properties.

**Tested:** IBM MQ, ActiveMQ Classic, ActiveMQ Artemis

## Problem

JMS properties are nested in JSON:

```json
{
  "properties": {
    "messageType": {
      "string": "PAYMENT"
    }
  }
}
```

Standard SMTs can't extract `properties.messageType.string`.

## Solutions

### JSON_PATH SMT (Recommended for SMT approach)

Use Confluent's built-in SMT:

```json
{
  "transforms": "routeByJsonPath",
  "transforms.routeByJsonPath.type": "io.confluent.connect.transforms.ExtractTopic$Value",
  "transforms.routeByJsonPath.field": "$[\"properties\"][\"messageType\"][\"string\"]",
  "transforms.routeByJsonPath.field.format": "JSON_PATH",
  "transforms.routeByJsonPath.skip.missing.or.null": "true"
}
```

**Flow:** `MQ → Connector with SMT → payment, transfer, notification topics`

**Pros:** Built-in, no custom code, sub-second  
**Cons:** No input topic

### Custom SMT (Historical)

This repo contains a custom SMT that does the same thing. **Use JSON_PATH instead** - it's simpler.

## Flink vs SMT

### Flink
```
MQ → Connector → jms.input → Flink → payment, transfer, notification
```

**Pros:** Exactly-once, input topic retained, aggregations  
**Cons:** Needs Flink cluster, 1-5 sec latency

### SMT (JSON_PATH or Custom)
```
MQ → Connector with SMT → payment, transfer, notification
```

**Pros:** No extra infrastructure, sub-second, simpler  
**Cons:** No input topic, no aggregations

### Comparison

| | **Flink** | **JSON_PATH SMT** | **Custom SMT** |
|-|-----------|-------------------|----------------|
| **Input topic?** | Yes | No | No |
| **Exactly-once?** | Yes | Connector-dependent | Connector-dependent |
| **Latency** | 1-5 sec | Sub-second | Sub-second |
| **Setup** | 15 min | 5 min | 30 min |
| **Custom code?** | No | No | Yes |
| **Aggregations?** | Yes | No | No |

[See Flink guide →](flink-routing/)
- ✅ Sub-second latency (routing at connector level)
- ❌ No audit trail in input topic
- ❌ Cannot re-process from original input
- ❌ Cannot have multiple routing strategies on same input

**Choose based on your requirements:**
- Need audit trail or re-processing? → **Apache Flink**
- Want minimal storage and direct routing? → **JSON_PATH SMT**

## Which to Use?

**Flink:** Need input topic for audit/replay? Need aggregations? → Use Flink  
**JSON_PATH SMT:** Simple routing, don't need input topic? → Use JSON_PATH SMT  
**Custom SMT:** Use JSON_PATH SMT instead (simpler, same result)

## Configuration

### JSON_PATH SMT

Add to your connector:

```json
{
  "transforms": "routeByJsonPath",
  "transforms.routeByJsonPath.type": "io.confluent.connect.transforms.ExtractTopic$Value",
  "transforms.routeByJsonPath.field": "$[\"properties\"][\"messageType\"][\"string\"]",
  "transforms.routeByJsonPath.field.format": "JSON_PATH",
  "transforms.routeByJsonPath.skip.missing.or.null": "true"
}
```

The SMT extracts `properties.messageType.string` and uses it as the topic name.

See [main README](../README.md#configuration) for full connector configs.

## Custom SMT Configuration (Historical)

**Note:** This approach is superseded by JSON_PATH SMT above. Use JSON_PATH instead.

Add these transforms to your IBM MQ Source Connector configuration:

```json
{
  "connector.class": "io.confluent.connect.ibm.mq.IbmMQSourceConnector",
  "kafka.topic": "ibm.mq.input",
  "jms.destination.name": "DEV.QUEUE.1",
  
  "transforms": "copyMessageType,routeByMessageType",
  
  "transforms.copyMessageType.type": "io.confluent.connect.transforms.JmsPropertyToHeader$Value",
  "transforms.copyMessageType.property.name": "messageType",
  "transforms.copyMessageType.header.name": "messageType",
  "transforms.copyMessageType.skip.missing": "true",
  
  "transforms.routeByMessageType.type": "io.confluent.connect.transforms.ExtractTopic$Header",
  "transforms.routeByMessageType.header": "messageType",
  "transforms.routeByMessageType.topic.format": "${topic}-topic",
  "transforms.routeByMessageType.skip.missing.or.null": "true"
}
```

### SMT Parameters

**JmsPropertyToHeader Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `property.name` | String | Required | Name of the JMS property to extract (e.g., "messageType") |
| `header.name` | String | Required | Name of the Kafka header to create |
| `skip.missing` | Boolean | true | If true, skip records where the property is missing or null. If false, throw an error. |

**ExtractTopic$Header Parameters:** (Confluent-provided SMT)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `header` | String | Required | Name of the header to extract (should match `header.name` above) |
| `topic.format` | String | `${topic}` | Format for the destination topic. Use `${topic}` as placeholder for header value |
| `skip.missing.or.null` | Boolean | false | Skip records where header is missing or null |

### Processing Flow

**Input message:**
```json
{
  "text": "{\"transactionId\":\"PAY-001\",\"amount\":5000}",
  "properties": {
    "messageType": {
      "propertyType": "string",
      "string": "PAYMENT"
    }
  }
}
```

**Step 1: JmsPropertyToHeader SMT** (custom, provided in this repository)
- Extracts: `properties.messageType.string = "PAYMENT"`
- Adds Kafka header: `messageType: "PAYMENT"`
- Message value: unchanged

**Step 2: ExtractTopic$Header SMT** (Confluent-provided)
- Reads header: `messageType = "PAYMENT"`
- Applies format: `"${topic}-topic"` → `"PAYMENT-topic"`
- Routes message to: `PAYMENT-topic`

**Result:**
- Topic: `PAYMENT-topic` (or `payment-topic` if header value is lowercase)
- Headers: `messageType: "PAYMENT"`
- Value: Original message unchanged

## Deployment to Confluent Cloud

### Option 1: Custom Connector Plugin (Recommended for Production)

1. Create a connector plugin ZIP:
```bash
mkdir -p kafka-connect-jms-smt/lib
cp target/jms-property-to-header-smt-1.0.0.jar kafka-connect-jms-smt/lib/
cd kafka-connect-jms-smt
zip -r ../jms-property-to-header-smt-plugin.zip .
```

2. Upload to Confluent Cloud:
   - Go to Confluent Cloud UI → Connectors → Custom connector plugins
   - Upload `jms-property-to-header-smt-plugin.zip`
   - Add to your connector configuration

## Quick Start

### Flink (15 minutes)

1. Open Flink SQL in Confluent Cloud
2. Run SQL from [flink-routing/routing.sql](flink-routing/routing.sql)

[Full guide →](flink-routing/)

### JSON_PATH SMT (5 minutes)

Add to connector config:

```json
{
  "transforms": "routeByJsonPath",
  "transforms.routeByJsonPath.type": "io.confluent.connect.transforms.ExtractTopic$Value",
  "transforms.routeByJsonPath.field": "$[\"properties\"][\"messageType\"][\"string\"]",
  "transforms.routeByJsonPath.field.format": "JSON_PATH",
  "transforms.routeByJsonPath.skip.missing.or.null": "true"
}
```

### Custom SMT (Historical)

Build JAR and upload. **Use JSON_PATH instead** - it's simpler.

---

## Custom SMT Reference (Historical)

**Note:** Use JSON_PATH SMT instead. This section kept for reference.

Build:
```bash
mvn clean package
```

Output: `target/jms-property-to-header-smt-1.0.0.jar`

Upload to Confluent Cloud as custom plugin.

1. Copy JAR to Kafka Connect plugin path:
```bash
mkdir -p /usr/local/share/kafka/plugins/jms-property-to-header-smt
cp target/jms-property-to-header-smt-1.0.0.jar /usr/local/share/kafka/plugins/jms-property-to-header-smt/
```

2. Update worker configuration:
```properties
plugin.path=/usr/local/share/kafka/plugins
```

3. Restart Kafka Connect and configure your connector with the transform

## What's in This Repo

```
jms-routing-smt/
├── flink-routing/          # Flink SQL routing (recommended)
├── src/                    # Custom SMT code (historical)
└── README.md              # This file
```

## Testing

Send JMS message with property:

```java
TextMessage msg = session.createTextMessage("{\"transactionId\":\"PAY-001\"}");
msg.setStringProperty("messageType", "PAYMENT");
producer.send(msg);
```

Verify message routed to `PAYMENT` topic.

