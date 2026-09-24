# JMS Property to Header SMT

Custom Kafka Connect Single Message Transform (SMT) to extract nested JMS properties from IBM MQ connector messages and route to different topics.

## Problem

The IBM MQ Source Connector stores JMS properties in a nested JSON structure:

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

**Challenge:** Route messages to different Kafka topics based on `properties.messageType.string`, but:
- Standard Kafka Connect SMTs cannot extract from nested JSON paths
- JMS properties are not copied to Kafka headers by default
- The value is deeply nested: `properties.messageType.string`

## Solution: Custom SMT

This repository provides a **custom Kafka Connect SMT** that:

1. **Extracts** nested JMS properties like `properties.messageType.string`
2. **Copies** the value to a Kafka message header
3. **Routes** messages using the standard `ExtractTopic$Header` SMT

### Architecture

```
IBM MQ → IBM MQ Source Connector
           ↓
         JmsPropertyToHeader (this custom SMT)
           ↓ (adds Kafka header: messageType=PAYMENT)
         ExtractTopic$Header (standard Kafka Connect SMT)
           ↓
         Routes to: payment-topic, transfer-topic, notification-topic
```

This approach keeps routing logic **inside Kafka Connect**, without requiring additional stream processing infrastructure.

## Recommended Alternatives

While this SMT works well, **Apache Flink or ksqlDB are recommended** for production use cases:

### Recommended: Apache Flink

**Why Flink?**
- **Exactly-once processing** guarantees
- **Event-time processing** with watermarks
- **Stateful operations** for complex routing
- **Better performance** at scale
- **Native support** in Confluent Cloud

**[See Flink solution →](flink-routing/)**

### Alternative: ksqlDB

**Why ksqlDB?**
- **Simplest deployment** (just SQL)
- **No code required**
- **5-minute setup** in Confluent Cloud
- **Good enough** for most use cases

**[See ksqlDB solution →](ksqldb-routing/)**

### This SMT Solution

**When to use the custom SMT:**
- You want routing **in the connector layer**
- You're running **self-managed** Kafka Connect
- You want to **minimize infrastructure** (no stream processing)
- You need **connector-level** processing semantics
- Limited support in Confluent Cloud (may require Enterprise plan)

### Quick Comparison

| Feature | **Flink** | **ksqlDB** | **Custom SMT** |
|---------|------------|-------------|-------------------|
| **Processing Semantics** | Exactly-once | At-least-once | Depends on connector |
| **Time to Deploy** | 15 minutes | 5 minutes | 30+ minutes |
| **Deployment** | SQL or Java | SQL only | Upload JAR + config |
| **Confluent Cloud** | Native | Native | Limited |
| **Code Required** | No (SQL) | No | Yes (Java) |
| **Windowing/Aggregation** | Advanced | Basic | No |
| **Stateful Processing** | Yes | Yes | No |
| **Performance** | Excellent | Good | Connector-limited |
| **Infrastructure** | Stream processor | Stream processor | None (in-connector) |

## When to Use This SMT

Choose this custom SMT approach if:

1. **You're already running self-managed Kafka Connect** and don't want to add stream processing infrastructure
2. **You want all routing logic in Kafka Connect** for architectural simplicity
3. **You have specific connector-level processing requirements** (e.g., dead letter queues, error handling)
4. **You cannot use Flink or ksqlDB** due to organizational constraints

For most users, especially those on Confluent Cloud, **we recommend Flink or ksqlDB** instead.

### SMT Configuration

Add these transforms to your IBM MQ Source Connector configuration:

```json
{
  "connector.class": "IbmMQSource",
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

**ExtractTopic$Header Parameters:** (Standard Kafka Connect SMT)

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

**Step 1: JmsPropertyToHeader SMT**
- Extracts: `properties.messageType.string = "PAYMENT"`
- Adds Kafka header: `messageType: "PAYMENT"`
- Message value: unchanged

**Step 2: ExtractTopic$Header SMT**
- Reads header: `messageType = "PAYMENT"`
- Applies format: `"${topic}-topic"` → `"payment-topic"`
- Routes message to: `payment-topic`

**Result:**
- Topic: `payment-topic`
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

## Decision Guide

**TL;DR: Use Flink if available, ksqlDB for simplicity, or this SMT if you must stay in Kafka Connect.**

### Choose Apache Flink if:
- You need **exactly-once processing** guarantees
- You're processing **financial transactions** or critical data
- You need **event-time processing** with watermarks
- You want **advanced windowing** and aggregations
- You have **high throughput** requirements (>100K msgs/sec)
- You're using **Confluent Cloud** (native support)

**Best for:** Production-grade streaming applications with strict processing guarantees.

**[Flink Routing Guide →](flink-routing/)**

### Choose ksqlDB if:
- You want the **fastest time to value** (5 minutes)
- You're using **Confluent Cloud** without Flink
- **SQL-only** development is preferred
- At-least-once processing is **acceptable**
- You need **basic filtering and routing**

**Best for:** Quick development, proof-of-concepts, and most streaming use cases.

**[ksqlDB Routing Guide →](ksqldb-routing/)**

### Choose This Custom SMT if:
- You want routing logic **in Kafka Connect**
- You're running **self-managed** Kafka Connect clusters
- You cannot use Flink or ksqlDB (organizational constraints)
- You need to **minimize infrastructure** (no stream processors)
- You're a **Kafka Connect purist**
- You understand Confluent Cloud has **limited custom SMT support**

**Best for:** Self-managed Kafka Connect deployments where stream processing is not an option.

**[Custom SMT Usage →](#usage-custom-smt)**

## Quick Start

### Fastest: ksqlDB (5 minutes) - Recommended for Quick Start

```sql
-- Connect to ksqlDB and run:
CREATE STREAM ibm_mq_input_stream WITH (
  KAFKA_TOPIC='ibm.mq.input',
  VALUE_FORMAT='JSON'
);

CREATE STREAM payment_topic AS
  SELECT text, properties->messageType->string AS messageType
  FROM ibm_mq_input_stream
  WHERE properties->messageType->string = 'PAYMENT'
  EMIT CHANGES;
```

Done! Messages with `messageType=PAYMENT` now flow to `payment-topic`.

**[See full ksqlDB guide →](ksqldb-routing/)**

### Best for Production: Apache Flink (15 minutes)

**Confluent Cloud for Apache Flink:**
1. Create a Flink compute pool
2. Open Flink SQL workspace  
3. Run the SQL from [`flink-routing/routing.sql`](flink-routing/routing.sql)

**Self-Managed Flink:**
```bash
./bin/sql-client.sh
# Then run SQL from flink-routing/routing.sql
```

**[See full Flink guide →](flink-routing/)**

### Custom SMT Approach (30+ minutes) - This Repository

**For self-managed Kafka Connect deployments:**

1. Build the JAR: `mvn clean package`
2. Upload `jms-property-to-header-smt-plugin.zip` to Kafka Connect
3. Configure connector with transforms

**[See full SMT usage →](#usage-custom-smt)**

---

## Usage: Custom SMT

This section covers using the custom SMT from this repository. **Consider using [Flink](#-best-for-production-apache-flink-15-minutes) or [ksqlDB](#-fastest-ksqldb-5-minutes---recommended-for-quick-start) instead for easier deployment.**

## Building

```bash
mvn clean package
```

Output: `target/jms-property-to-header-smt-1.0.0.jar`

## Testing Locally

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

## Architecture Overview

### Custom SMT Routing (This Repository)
```
IBM MQ → IBM MQ Source Connector
           ↓
         [Transform Chain]
           ↓
         JmsPropertyToHeader (custom SMT - this repo)
           ↓ (adds Kafka header: messageType=PAYMENT)
         ExtractTopic$Header (standard Kafka Connect SMT)
           ↓
         Routes to: payment-topic, transfer-topic, notification-topic

All processing happens within Kafka Connect - no external stream processors needed
```

### Apache Flink Routing (Recommended Alternative)
```
IBM MQ → MQ Connector → ibm.mq.input topic
                             ↓
                      Flink Job
                 (exactly-once semantics,
                  event-time processing)
                         /  |  \
                        /   |   \
           payment-topic  transfer-topic  notification-topic

Separate stream processor - better semantics, more capabilities
```

### ksqlDB Routing (Simpler Alternative)
```
IBM MQ → MQ Connector → ibm.mq.input topic
                             ↓
                         ksqlDB
                    (SQL streaming queries,
                     at-least-once)
                         /  |  \
                        /   |   \
           payment-topic  transfer-topic  notification-topic

Separate stream processor - simplest deployment, SQL-only
```

## Repository Structure

```
.
├── README.md                          # This file - overview and decision guide
│
├── src/                               # Custom SMT (primary focus)
│   └── main/java/io/confluent/connect/transforms/
│       └── JmsPropertyToHeader.java   # Custom SMT implementation
├── pom.xml                            # Maven build for custom SMT
├── jms-property-to-header-smt-plugin.zip  # Ready-to-upload connector plugin
│
├── flink-routing/                     # Apache Flink (recommended alternative)
│   ├── README.md                      # Full Flink guide
│   ├── routing.sql                    # Flink SQL DDL and routing
│   └── java-router/                   # Flink Table API (Java)
│       ├── pom.xml
│       └── src/main/java/io/confluent/flink/FlinkMessageRouter.java
│
└── ksqldb-routing/                    # ksqlDB (simpler alternative)
    ├── README.md                      # Full ksqlDB guide
    └── routing.sql                    # ksqlDB SQL statements
```

**Primary deliverable:** Custom Kafka Connect SMT for routing IBM MQ messages  
**Bonus content:** Flink and ksqlDB alternatives for teams with stream processing infrastructure

## Real-World Considerations

### Performance Characteristics

| Solution | Latency | Throughput | Resource Usage |
|----------|---------|------------|----------------|
| **Custom SMT** | ~10-50ms | Connector-limited* | Lowest (in-connector) |
| **Flink** | ~20-100ms | 100K-1M+ msgs/sec | Higher (CFU-based) |
| **ksqlDB** | ~50-200ms | 10K-100K msgs/sec | Moderate (CSU-based) |

*SMT throughput is limited by connector throughput, typically 10K-50K msgs/sec per task.

### Operational Complexity

| Solution | Setup | Monitoring | Troubleshooting | Updates |
|----------|-------|------------|-----------------|---------|
| **Custom SMT** | Complex | Connector metrics | Worker logs | Update config + restart |
| **ksqlDB** | Easy | Built-in UI | SQL logs | Edit SQL (live) |
| **Flink** | Moderate | Web UI + metrics | Job logs | Redeploy job |

### Deployment & Cost (Confluent Cloud)

| Solution | Confluent Cloud Support | Pricing Model | Infrastructure |
|----------|-------------------------|---------------|----------------|
| **Custom SMT** | Limited (Enterprise plan may be required) | Included in connector | Kafka Connect workers |
| **Flink** | Native support | CFU-based (higher cost, better guarantees) | Managed Flink cluster |
| **ksqlDB** | Native support | CSU-based (moderate cost) | Managed ksqlDB cluster |

### Processing Guarantees

| Solution | Semantics | State Management | Failure Recovery |
|----------|-----------|------------------|------------------|
| **Custom SMT** | Connector-dependent (typically at-least-once) | None (stateless) | Connector restart |
| **Flink** | **Exactly-once** | RocksDB checkpoints | Automatic from checkpoint |
| **ksqlDB** | At-least-once | RocksDB | Automatic from offset |

### When Each Solution Shines

**Custom SMT:**
- Minimal infrastructure footprint
- Self-managed Kafka Connect
- Simple routing without aggregations
- No windowing or stateful operations
- Limited Confluent Cloud support

**Flink (Recommended for Production):**
- **Exactly-once processing** (critical for financial data)
- **High throughput** (100K+ msgs/sec)
- **Event-time processing** with watermarks
- **Advanced features** (windowing, joins, state)
- **Native Confluent Cloud** support
- Higher cost

**ksqlDB (Recommended for Quick Start):**
- **Fastest deployment** (5 minutes)
- **SQL-only** (no code)
- **Good enough** for most use cases
- **Native Confluent Cloud** support
- At-least-once semantics
- Lower throughput than Flink

## Common Use Cases

### Use Case 1: Simple Routing
**Requirement:** Route messages to 3 topics based on messageType  
**Recommendation:** ksqlDB ⭐  
**Why:** Simplest solution, no code required

### Use Case 2: Routing + Aggregation
**Requirement:** Route messages AND count by type per minute  
**Recommendation:** Flink or ksqlDB ⭐  
**Why:** Both support windowing

### Use Case 3: Exactly-Once Financial Transactions
**Requirement:** Process payments with no duplicates  
**Recommendation:** Flink ⭐  
**Why:** Exactly-once semantics

### Use Case 4: High-Volume Low-Latency
**Requirement:** 500K msgs/sec with <50ms latency  
**Recommendation:** Flink ⭐  
**Why:** Best performance characteristics

### Use Case 5: Minimal Infrastructure
**Requirement:** Already running Kafka Connect, avoid new components  
**Recommendation:** Custom SMT ⭐  
**Why:** No additional infrastructure

## Testing

Send test messages with the Java JMS client:

```java
import com.ibm.mq.jms.*;
import javax.jms.*;

MQConnectionFactory cf = new MQConnectionFactory();
cf.setHostName("localhost");
cf.setPort(1414);
cf.setQueueManager("QM1");
cf.setChannel("CONFLUENT.CHL");

Connection conn = cf.createConnection("username", "password");
Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
Queue queue = session.createQueue("DEV.QUEUE.1");
MessageProducer producer = session.createProducer(queue);

// Send message with messageType property
TextMessage msg = session.createTextMessage("{\"transactionId\":\"PAY-001\",\"amount\":5000}");
msg.setStringProperty("messageType", "PAYMENT");  // This becomes properties.messageType.string
producer.send(msg);
```

## Troubleshooting

### Messages not routing to correct topics

**Check:**
1. Verify JMS property is set: Look for `properties.messageType.string` in Kafka message
2. Check property value matches exactly (case-sensitive): `PAYMENT` ≠ `payment`
3. Verify routing queries are running (ksqlDB/Flink) or SMT is configured

### All messages going to unknown topic

**Cause:** Property path might be wrong or null  
**Solution:** Inspect a sample message to verify structure

### High latency

**ksqlDB:** Check query backlog in UI  
**Flink:** Check backpressure in Flink Web UI  
**SMT:** Check connector task lag

## Contributing

Contributions welcome! Please:
1. Test with IBM MQ 9.4+ and Confluent Platform 7.5+
2. Follow existing code style
3. Add tests for new features
4. Update documentation

## License

Apache 2.0

## Credits

Built for routing IBM MQ messages in Confluent environments. Supports Confluent Cloud and self-managed deployments.
