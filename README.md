# JMS SMT Message Router Examples

Single Message Transformation (SMT) examples for routing JMS messages from an aggregated queue to multiple Kafka topics based on message headers.

**Supported JMS Providers:**
- IBM MQ (IBM MQ Source Connector)
- Apache ActiveMQ Classic (ActiveMQ Source Connector)
- Apache ActiveMQ Artemis (ActiveMQ Source Connector)

## Overview

This repository provides **SMT configuration examples** for JMS Source Connectors to route messages from a single aggregated JMS queue to multiple Kafka topics based on message metadata.

### Use Case

When you have a **single JMS queue** containing messages from multiple sources/applications, you need to route messages to different Kafka topics based on their content or metadata. This is achieved using Confluent's `RegexRouter` SMT and other Single Message Transformations.

### How It Works

1. **JMS messages include properties** (e.g., `messageType`, `priority`) that indicate message category
2. **JMS Source Connector** reads from the aggregated queue and automatically converts JMS properties → Kafka headers
3. **SMT Routing:** `RegexRouter` SMT reads Kafka headers (e.g., `messageType`) and routes to the appropriate topic
4. **No custom code required** - purely configuration-based routing

```
JMS Message Properties         Kafka Headers              SMT Routing
──────────────────────        ────────────────           ────────────
messageType=PAYMENT    →      messageType: PAYMENT   →   Topic: PAYMENT
priority=HIGH                 priority: HIGH
```

## Common Source Patterns for Message Aggregation

This repository assumes you have a **single JMS queue** containing messages from multiple sources. Common patterns for achieving this aggregation:

### IBM MQ Streaming Queues (IBM MQ 9.2.3+)
Non-disruptive message duplication using the `STREAMQ` property. Legacy applications continue reading from original queues while messages are automatically cloned to an aggregation queue for Kafka ingestion.

### ActiveMQ Network of Brokers (ActiveMQ Classic 5.x)
Hub-and-spoke topology where spoke brokers forward messages to a central hub broker. The connector reads from the hub broker's aggregated queue. Can also use Composite Destinations for message duplication at the queue level.

### Artemis Core Hub (ActiveMQ Artemis 2.x+)
Federation-based hub where spoke brokers redistribute messages to a central hub. The connector reads from federated queues on the hub broker, providing a centralized consumption point.

---

**Note:** Setup of these aggregation patterns is outside the scope of this repository. This guide focuses on SMT routing patterns that work with any aggregated JMS queue.

## Quick Example

### Basic Header-Based Routing

Add this SMT configuration to your JMS Source Connector:

```json
{
  "connector.class": "io.confluent.connect.ibm.mq.IbmMQSourceConnector",
  "kafka.topic": "jms-default",
  
  "transforms": "route",
  "transforms.route.type": "io.confluent.connect.transforms.ExtractTopic$Header",
  "transforms.route.field": "messageType",
  "transforms.route.skip.missing.or.null": "true"
}
```

**How it works:**
1. The JMS Source Connector converts JMS message properties to Kafka headers
2. The `ExtractTopic$Header` SMT reads the `messageType` header value
3. That value becomes the destination topic name
4. Messages without the header fall back to the `kafka.topic` setting

**Result:**
- Message with `messageType: PAYMENT_DOMESTIC` → Routes to `PAYMENT_DOMESTIC` topic
- Message with `messageType: FRAUD_ALERT` → Routes to `FRAUD_ALERT` topic
- Message without `messageType` header → Routes to `jms-default` topic (fallback)

**Note:** SMT configuration (`transforms` section) is identical across all JMS providers. Only connector-specific properties differ.

## Configuration Examples

See the [examples/](examples/) directory for complete configuration examples. Each example is available for both IBM MQ and ActiveMQ:
- **IBM MQ:** [examples/ibm-mq/](examples/ibm-mq/)
- **ActiveMQ:** [examples/activemq/](examples/activemq/)

### Banking Use Case Examples

| Example | Description | Use When |
|---------|-------------|----------|
| **basic-routing.json** | Simple routing by messageType header | Single header determines topic |
| **routing-with-prefix.json** | Add prefix to routed topics | Want to namespace topics (e.g., `banking-PAYMENT`) |
| **multi-dimensional-routing.json** | Route by combined routing key | Need topics like `RETAIL-PAYMENT` (requires app to set routingKey property) |
| **conditional-routing.json** | Route based on predicates | Different routing rules for different message types |
| **routing-with-metadata.json** | Add enrichment before routing | Need to add timestamp, source info, etc. |

**Note:** SMT configuration is identical across both providers. Only connector-specific properties (connection details, credentials) differ.

## Sample Message: How Routing Works

Let's see how a single MQ message gets routed differently by each pattern.

### Sample MQ Message

**Publishing to MQ (Java):**
```java
TextMessage message = session.createTextMessage("{\"transactionId\":\"TXN-12345\",\"amount\":1500.00}");

// Set MQ properties (these become Kafka headers after duplication)
message.setStringProperty("messageType", "PAYMENT");
message.setStringProperty("priority", "HIGH");
message.setStringProperty("routingKey", "RETAIL-PAYMENT");  // Pre-combined for Pattern 3

// Application publishes to its normal queue
Queue appQueue = session.createQueue("PAYMENT.APP.QUEUE");
sender.send(appQueue, message);

// MQ automatically duplicates to KAFKA.AGGREGATION.QUEUE (via STREAMQ property)
```

**Resulting Kafka Headers (after connector processing):**
```
messageType: PAYMENT
priority: HIGH
routingKey: RETAIL-PAYMENT
```

**Message Payload:**
```json
{"transactionId":"TXN-12345","amount":1500.00}
```

### How Each Pattern Routes This Message

| Pattern | SMT Used | Resulting Topic | Why |
|---------|----------|-----------------|-----|
| **Pattern 1: Basic** | `ExtractTopic$Header(messageType)` | `PAYMENT` | Extracts messageType header value directly |
| **Pattern 2: Prefix** | `ExtractTopic$Header` + `RegexRouter` | `banking-PAYMENT` | Extracts header, then adds prefix |
| **Pattern 3: Multi-dimensional** | `ExtractTopic$Header(routingKey)` | `RETAIL-PAYMENT` | Uses pre-combined routing key |
| **Pattern 4: Enrichment** | `InsertField` + `ExtractTopic$Header` | `PAYMENT` | Enriches payload, then routes by messageType |
| **Pattern 5: Conditional** | `ExtractTopic$Header` + predicate | `PAYMENT-priority` | Routes to priority topic (has priority header) |

**Pattern 4 Enriched Payload:**
```json
{
  "transactionId":"TXN-12345",
  "amount":1500.00,
  "ingestedAt":"2026-08-25T10:30:00Z",
  "sourceSystem":"CORE_BANKING_MQ"
}
```

## SMT Configuration Patterns

### Pattern 1: Basic Routing

Route based on a single header value using `ExtractTopic$Header`:

```json
"transforms": "route",
"transforms.route.type": "io.confluent.connect.transforms.ExtractTopic$Header",
"transforms.route.field": "messageType",
"transforms.route.skip.missing.or.null": "true"
```

**Result:**
- Message with `messageType: PAYMENT_DOMESTIC` → Routes to `PAYMENT_DOMESTIC` topic
- Message with `messageType: PAYMENT_INTERNATIONAL` → Routes to `PAYMENT_INTERNATIONAL` topic
- Message with `messageType: FRAUD_ALERT` → Routes to `FRAUD_ALERT` topic
- Message without `messageType` header → Routes to fallback topic (configured as `kafka.topic`)

### Pattern 2: Routing with Topic Prefix

Add a namespace prefix to all routed topics using an SMT chain:

```json
"transforms": "extractTopic,addPrefix",

"transforms.extractTopic.type": "io.confluent.connect.transforms.ExtractTopic$Header",
"transforms.extractTopic.field": "messageType",
"transforms.extractTopic.skip.missing.or.null": "true",

"transforms.addPrefix.type": "org.apache.kafka.connect.transforms.RegexRouter",
"transforms.addPrefix.regex": ".*",
"transforms.addPrefix.replacement": "banking-$0"
```

**Result:**
- Message with `messageType: PAYMENT_DOMESTIC` → Routes to `banking-PAYMENT_DOMESTIC` topic
- Message with `messageType: FRAUD_ALERT` → Routes to `banking-FRAUD_ALERT` topic
- Message with `messageType: ACCOUNT_TRANSACTION` → Routes to `banking-ACCOUNT_TRANSACTION` topic
- All topics are prefixed with `banking-` to create a clear namespace for JMS-sourced messages

### Pattern 3: Multi-Dimensional Routing

Route using a pre-combined routing key header:

```json
"transforms": "route",
"transforms.route.type": "io.confluent.connect.transforms.ExtractTopic$Header",
"transforms.route.field": "routingKey",
"transforms.route.skip.missing.or.null": "true"
```

**Application Requirement:**
The application must set a single `routingKey` JMS property with the combined value:
```java
message.setStringProperty("routingKey", "RETAIL-PAYMENT");
```

**Result:**
- Message with `routingKey: RETAIL-PAYMENT` → Routes to `RETAIL-PAYMENT` topic
- Message with `routingKey: CORPORATE-PAYMENT` → Routes to `CORPORATE-PAYMENT` topic
- Message with `routingKey: RETAIL-FRAUD_ALERT` → Routes to `RETAIL-FRAUD_ALERT` topic
- Each business unit gets separate topics for each message type, enabling independent processing and retention policies

**Note:** Standard SMTs cannot combine multiple headers dynamically. Applications must pre-combine values into a single routing header.

### Pattern 4: Routing with Metadata Enrichment

Add metadata before routing using an SMT chain:

```json
"transforms": "addTimestamp,addSource,route",

"transforms.addTimestamp.type": "org.apache.kafka.connect.transforms.InsertField$Value",
"transforms.addTimestamp.timestamp.field": "ingestedAt",

"transforms.addSource.type": "org.apache.kafka.connect.transforms.InsertField$Value",
"transforms.addSource.static.field": "sourceSystem",
"transforms.addSource.static.value": "CORE_BANKING_MQ",

"transforms.route.type": "io.confluent.connect.transforms.ExtractTopic$Header",
"transforms.route.field": "messageType",
"transforms.route.skip.missing.or.null": "true"
```

**Result:**
- Messages are enriched with additional fields BEFORE routing
- Original message: `{"transactionId": "123", "amount": 1000}`
- Enriched message: `{"transactionId": "123", "amount": 1000, "ingestedAt": "2026-08-21T10:30:00Z", "sourceSystem": "CORE_BANKING_MQ"}`
- Then routed by `messageType` to appropriate topic (e.g., `PAYMENT_DOMESTIC`)
- Downstream consumers receive enriched messages with audit trail and source tracking built-in

### Pattern 5: Conditional Routing with Predicates

Apply different routing rules based on header presence:

```json
"transforms": "extractTopic,addPrioritySuffix,extractTopicNormal",
"predicates": "hasPriorityHeader",

"predicates.hasPriorityHeader.type": "org.apache.kafka.connect.transforms.predicates.HasHeaderKey",
"predicates.hasPriorityHeader.name": "priority",

"transforms.extractTopic.type": "io.confluent.connect.transforms.ExtractTopic$Header",
"transforms.extractTopic.field": "messageType",
"transforms.extractTopic.skip.missing.or.null": "true",
"transforms.extractTopic.predicate": "hasPriorityHeader",

"transforms.addPrioritySuffix.type": "org.apache.kafka.connect.transforms.RegexRouter",
"transforms.addPrioritySuffix.regex": ".*",
"transforms.addPrioritySuffix.replacement": "$0-priority",
"transforms.addPrioritySuffix.predicate": "hasPriorityHeader",

"transforms.extractTopicNormal.type": "io.confluent.connect.transforms.ExtractTopic$Header",
"transforms.extractTopicNormal.field": "messageType",
"transforms.extractTopicNormal.skip.missing.or.null": "true",
"transforms.extractTopicNormal.predicate": "hasPriorityHeader",
"transforms.extractTopicNormal.negate": "true"
```

**Result:**
- Message with `priority` header + `messageType: PAYMENT` → Routes to `PAYMENT-priority` topic
- Message with `priority` header + `messageType: FRAUD_ALERT` → Routes to `FRAUD_ALERT-priority` topic
- Message with `messageType: PAYMENT` (no priority header) → Routes to `PAYMENT` topic
- Message with `messageType: ACCOUNT_TRANSACTION` (no priority header) → Routes to `ACCOUNT_TRANSACTION` topic
- Messages with priority header get dedicated topics for faster processing, separate consumer groups, and stricter SLAs

**Note:** `HasHeaderKey` only checks for header existence, not value. All messages with a `priority` header (regardless of value) will route to priority topics.

## IBM MQ Streaming Queue Configuration

**Requirement:** IBM MQ 9.2.3 or later

To set up message duplication using Streaming Queues:

### 1. Create Aggregation Queue

```mqsc
DEFINE QLOCAL(KAFKA.AGGREGATION.QUEUE) MAXDEPTH(100000)
```

### 2. Configure Streaming on Application Queues

Point your existing application queues to duplicate messages to the aggregation queue:

```mqsc
ALTER QLOCAL(PAYMENT.APP.QUEUE) STREAMQ(KAFKA.AGGREGATION.QUEUE) STRMQOS(BESTEF)
ALTER QLOCAL(ACCOUNT.APP.QUEUE) STREAMQ(KAFKA.AGGREGATION.QUEUE) STRMQOS(BESTEF)
ALTER QLOCAL(FRAUD.APP.QUEUE) STREAMQ(KAFKA.AGGREGATION.QUEUE) STRMQOS(BESTEF)
```

**Configuration Options:**
- `STREAMQ`: Target queue for duplicated messages
- `STRMQOS(BESTEF)`: Best Effort quality of service - if aggregation queue fills up, it won't block the original application queue

### 3. Configure Connector to Read from Aggregation Queue

```json
{
  "connector.class": "io.confluent.connect.ibm.mq.IbmMQSourceConnector",
  "jms.destination.name": "KAFKA.AGGREGATION.QUEUE",
  "jms.destination.type": "queue"
}
```

**Result:** Legacy applications continue using their queues unchanged, while Kafka receives a real-time copy of all messages via the aggregation queue.

## Prerequisites

This assumes you already have:
- JMS message broker environment with message aggregation configured (Streaming Queues, Network of Brokers, or Federation)
- JMS Source Connector deployed in Confluent Cloud (IBM MQ, ActiveMQ, or Artemis)
- JMS messages with routing properties set (e.g., `messageType`)
- Kafka topics created (or auto-creation enabled)

**Connector Documentation:**
- [IBM MQ Source Connector](https://docs.confluent.io/kafka-connectors/ibm-mq-source/current/overview.html)
- [ActiveMQ Source Connector](https://docs.confluent.io/kafka-connectors/activemq-source/current/overview.html)

## Important: JMS Message Properties

For routing to work, your JMS messages **must** include the properties you're routing on.

**Example: Setting JMS properties in Java:**

```java
TextMessage message = session.createTextMessage(payload);
message.setStringProperty("messageType", "PAYMENT_DOMESTIC");
message.setStringProperty("priority", "HIGH");
message.setStringProperty("businessUnit", "RETAIL");

// Application publishes to its normal queue
Queue appQueue = session.createQueue("PAYMENT.APP.QUEUE");
sender.send(appQueue, message);
```

**What happens:**
1. Application publishes to `PAYMENT.APP.QUEUE` (business as usual)
2. JMS broker duplicates/forwards the message (with all properties) to `KAFKA.AGGREGATION.QUEUE` (via Streaming Queues, Network of Brokers, or Federation)
3. JMS Source Connector reads from `KAFKA.AGGREGATION.QUEUE` and converts JMS properties to Kafka headers
4. SMTs use the Kafka headers for routing

## Error Handling

### Dead Letter Queue (DLQ)

Configure DLQ to catch routing errors:

```json
"kafka.topic": "jms-unrouted-dlq",

"errors.tolerance": "all",
"errors.deadletterqueue.topic.name": "jms-error-dlq",
"errors.deadletterqueue.context.headers.enable": "true"
```

- **kafka.topic** (fallback): Messages without routing headers go here
- **errors.deadletterqueue.topic.name**: Messages that fail processing go here

## Troubleshooting

### Messages going to wrong topic
- Check the header value matches topic name exactly (case-sensitive)
- Verify JMS property is being set correctly
- Check connector logs for routing decisions

### Messages going to default topic instead of being routed
- Verify header exists on the message (check in Confluent Cloud UI)
- Confirm JMS message has the property set
- For IBM MQ connector: Check connector config has `mq.message.body.jms: "true"`

### Topic not found errors
- Enable auto topic creation, OR
- Pre-create all expected topics, OR
- Use DLQ to catch messages for non-existent topics

## Resources

**Connectors:**
- [Confluent IBM MQ Source Connector](https://docs.confluent.io/kafka-connectors/ibm-mq-source/current/overview.html)
- [Confluent ActiveMQ Source Connector](https://docs.confluent.io/kafka-connectors/activemq-source/current/overview.html)

**Single Message Transformations:**
- [RegexRouter SMT Documentation](https://docs.confluent.io/platform/current/connect/transforms/regexrouter.html)
- [Kafka Connect Transformations](https://docs.confluent.io/platform/current/connect/transforms/overview.html)
- [Kafka Connect Predicates](https://docs.confluent.io/platform/current/connect/transforms/predicates.html)

---

## Appendix: IBM MQ Streaming Queue Setup

This section provides detailed setup instructions for IBM MQ Streaming Queues. This is one way to create the aggregated queue that the connector reads from.

**Requirement:** IBM MQ 9.2.3 or later

### 1. Create Aggregation Queue

```mqsc
DEFINE QLOCAL(KAFKA.AGGREGATION.QUEUE) MAXDEPTH(100000)
```

### 2. Configure Streaming on Application Queues

Point your existing application queues to duplicate messages to the aggregation queue:

```mqsc
ALTER QLOCAL(PAYMENT.APP.QUEUE) STREAMQ(KAFKA.AGGREGATION.QUEUE) STRMQOS(BESTEF)
ALTER QLOCAL(ACCOUNT.APP.QUEUE) STREAMQ(KAFKA.AGGREGATION.QUEUE) STRMQOS(BESTEF)
ALTER QLOCAL(FRAUD.APP.QUEUE) STREAMQ(KAFKA.AGGREGATION.QUEUE) STRMQOS(BESTEF)
```

**Configuration Options:**
- `STREAMQ`: Target queue for duplicated messages
- `STRMQOS(BESTEF)`: Best Effort quality of service - if aggregation queue fills up, it won't block the original application queue

### 3. Configure Connector to Read from Aggregation Queue

```json
{
  "connector.class": "io.confluent.connect.ibm.mq.IbmMQSourceConnector",
  "jms.destination.name": "KAFKA.AGGREGATION.QUEUE",
  "jms.destination.type": "queue"
}
```

**Result:** Legacy applications continue using their queues unchanged, while Kafka receives a real-time copy of all messages via the aggregation queue.

---

## Contributing

This repository contains reference examples. Feel free to adapt these patterns to your specific use case.

## License

See [LICENSE](LICENSE) file for details.
