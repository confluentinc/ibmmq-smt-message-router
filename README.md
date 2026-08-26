# IBM MQ SMT Message Router Examples

Single Message Transformation (SMT) examples for routing IBM MQ messages to different Kafka topics based on message headers.

## Overview

This repository provides **SMT configuration examples** for the IBM MQ Source Connector to route messages from a single MQ aggregation queue to multiple Kafka topics based on message metadata.

### Use Case

When using **IBM MQ Streaming Queues** (introduced in IBM MQ 9.2.3+) to duplicate messages from multiple application queues into a single aggregation queue for Kafka, you need to route messages to different Kafka topics based on their content or metadata. This is achieved using Confluent's `RegexRouter` SMT.

**The Streaming Queue Pattern:**
- Legacy applications continue reading from their original queues (e.g., `APP1.QUEUE`, `APP2.QUEUE`)
- IBM MQ automatically duplicates messages to an aggregation queue (e.g., `KAFKA.AGGREGATION.QUEUE`) using the `STREAMQ` property
- The IBM MQ Source Connector reads from the aggregation queue
- SMTs route duplicated messages to appropriate Kafka topics based on message properties
- **Non-disruptive:** Legacy systems are unaffected; Kafka gets a consolidated real-time feed

### How It Works

1. **Streaming Queue Setup:** Configure `STREAMQ` property on application queues to duplicate messages to `KAFKA.AGGREGATION.QUEUE`
2. **Message Duplication:** When applications put messages on their queues, MQ automatically clones messages (including headers and payload) to the aggregation queue
3. **Connector Ingestion:** IBM MQ Source Connector reads from the aggregation queue and converts MQ properties → Kafka headers
4. **SMT Routing:** `RegexRouter` SMT reads Kafka headers (e.g., `messageType`) and routes to the appropriate topic
5. **No custom code required** - purely configuration-based routing

```
MQ Message Properties          Kafka Headers              SMT Routing
─────────────────────         ────────────────           ────────────
messageType=PAYMENT    →      messageType: PAYMENT   →   Topic: PAYMENT
priority=HIGH                 priority: HIGH
```

## Quick Example

### Basic Header-Based Routing

Add this SMT configuration to your IBM MQ Source Connector:

```json
{
  "connector.class": "io.confluent.connect.ibm.mq.IbmMQSourceConnector",
  "kafka.topic": "mq-default",
  
  "transforms": "route",
  "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
  "transforms.route.regex": ".*",
  "transforms.route.replacement": "${header:messageType}"
}
```

**Result:**
- Message with `messageType: PAYMENT_DOMESTIC` → Routes to `PAYMENT_DOMESTIC` topic
- Message with `messageType: FRAUD_ALERT` → Routes to `FRAUD_ALERT` topic
- Message without `messageType` header → Routes to `mq-default` topic (fallback)

## Configuration Examples

See the [examples/](examples/) directory for complete configuration examples:

### Banking Use Case Examples

| Example | Description | Use When |
|---------|-------------|----------|
| **basic-routing.json** | Simple routing by messageType header | Single header determines topic |
| **routing-with-prefix.json** | Add prefix to routed topics | Want to namespace topics (e.g., `banking-PAYMENT`) |
| **multi-dimensional-routing.json** | Route by multiple headers | Need topics like `RETAIL-PAYMENT` |
| **conditional-routing.json** | Route based on predicates | Different routing rules for different message types |
| **routing-with-metadata.json** | Add enrichment before routing | Need to add timestamp, source info, etc. |

## Sample Message: How Routing Works

Let's see how a single MQ message gets routed differently by each pattern.

### Sample MQ Message

**Publishing to MQ (Java):**
```java
TextMessage message = session.createTextMessage("{\"transactionId\":\"TXN-12345\",\"amount\":1500.00}");

// Set MQ properties (these become Kafka headers after duplication)
message.setStringProperty("messageType", "PAYMENT");
message.setStringProperty("priority", "HIGH");
message.setStringProperty("businessUnit", "RETAIL");

// Application publishes to its normal queue
Queue appQueue = session.createQueue("PAYMENT.APP.QUEUE");
sender.send(appQueue, message);

// MQ automatically duplicates to KAFKA.AGGREGATION.QUEUE (via STREAMQ property)
```

**Resulting Kafka Headers (after connector processing):**
```
messageType: PAYMENT
priority: HIGH
businessUnit: RETAIL
```

**Message Payload:**
```json
{"transactionId":"TXN-12345","amount":1500.00}
```

### How Each Pattern Routes This Message

| Pattern | Configuration | Resulting Topic | Why |
|---------|--------------|-----------------|-----|
| **Pattern 1: Basic** | `${header:messageType}` | `PAYMENT` | Uses messageType header directly |
| **Pattern 2: Prefix** | `banking-${header:messageType}` | `banking-PAYMENT` | Adds namespace prefix |
| **Pattern 3: Multi-dimensional** | `${header:businessUnit}-${header:messageType}` | `RETAIL-PAYMENT` | Combines two headers |
| **Pattern 4: Enrichment** | `${header:messageType}` (after enrichment) | `PAYMENT` | Same routing, but payload enriched first |
| **Pattern 5: Conditional** | `${header:messageType}-priority` (has priority header) | `PAYMENT-priority` | Routes to priority topic |

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

Route based on a single header value:

```json
"transforms": "route",
"transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
"transforms.route.regex": ".*",
"transforms.route.replacement": "${header:messageType}"
```

**Result:**
- Message with `messageType: PAYMENT_DOMESTIC` → Routes to `PAYMENT_DOMESTIC` topic
- Message with `messageType: PAYMENT_INTERNATIONAL` → Routes to `PAYMENT_INTERNATIONAL` topic
- Message with `messageType: FRAUD_ALERT` → Routes to `FRAUD_ALERT` topic
- Message without `messageType` header → Routes to `mq-default` topic (fallback)

### Pattern 2: Routing with Topic Prefix

Add a namespace prefix to all routed topics:

```json
"transforms": "route",
"transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
"transforms.route.regex": ".*",
"transforms.route.replacement": "banking-${header:messageType}"
```

**Result:**
- Message with `messageType: PAYMENT_DOMESTIC` → Routes to `banking-PAYMENT_DOMESTIC` topic
- Message with `messageType: FRAUD_ALERT` → Routes to `banking-FRAUD_ALERT` topic
- Message with `messageType: ACCOUNT_TRANSACTION` → Routes to `banking-ACCOUNT_TRANSACTION` topic
- All topics are prefixed with `banking-` to create a clear namespace for MQ-sourced messages

### Pattern 3: Multi-Dimensional Routing

Combine multiple headers for topic name:

```json
"transforms": "route",
"transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
"transforms.route.regex": ".*",
"transforms.route.replacement": "${header:businessUnit}-${header:messageType}"
```

**Result:**
- Message with `businessUnit: RETAIL` + `messageType: PAYMENT` → Routes to `RETAIL-PAYMENT` topic
- Message with `businessUnit: CORPORATE` + `messageType: PAYMENT` → Routes to `CORPORATE-PAYMENT` topic
- Message with `businessUnit: RETAIL` + `messageType: FRAUD_ALERT` → Routes to `RETAIL-FRAUD_ALERT` topic
- Each business unit gets separate topics for each message type, enabling independent processing and retention policies

### Pattern 4: Routing with Metadata Enrichment

Add metadata before routing:

```json
"transforms": "addTimestamp,addSource,route",

"transforms.addTimestamp.type": "org.apache.kafka.connect.transforms.InsertField$Value",
"transforms.addTimestamp.timestamp.field": "ingestedAt",

"transforms.addSource.type": "org.apache.kafka.connect.transforms.InsertField$Value",
"transforms.addSource.static.field": "sourceSystem",
"transforms.addSource.static.value": "CORE_BANKING_MQ",

"transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
"transforms.route.regex": ".*",
"transforms.route.replacement": "${header:messageType}"
```

**Result:**
- Messages are enriched with additional fields BEFORE routing
- Original message: `{"transactionId": "123", "amount": 1000}`
- Enriched message: `{"transactionId": "123", "amount": 1000, "ingestedAt": "2026-08-21T10:30:00Z", "sourceSystem": "CORE_BANKING_MQ"}`
- Then routed by `messageType` to appropriate topic (e.g., `PAYMENT_DOMESTIC`)
- Downstream consumers receive enriched messages with audit trail and source tracking built-in

### Pattern 5: Conditional Routing with Predicates

Apply different routing rules based on conditions:

```json
"transforms": "routeHighPriority,routeNormal",
"predicates": "isHighPriority",

"predicates.isHighPriority.type": "org.apache.kafka.connect.transforms.predicates.HasHeaderKey",
"predicates.isHighPriority.name": "priority",

"transforms.routeHighPriority.type": "org.apache.kafka.connect.transforms.RegexRouter",
"transforms.routeHighPriority.regex": ".*",
"transforms.routeHighPriority.replacement": "${header:messageType}-priority",
"transforms.routeHighPriority.predicate": "isHighPriority",

"transforms.routeNormal.type": "org.apache.kafka.connect.transforms.RegexRouter",
"transforms.routeNormal.regex": ".*",
"transforms.routeNormal.replacement": "${header:messageType}",
"transforms.routeNormal.predicate": "isHighPriority",
"transforms.routeNormal.negate": "true"
```

**Result:**
- Message with `priority: HIGH` + `messageType: PAYMENT` → Routes to `PAYMENT-priority` topic
- Message with `priority: CRITICAL` + `messageType: FRAUD_ALERT` → Routes to `FRAUD_ALERT-priority` topic
- Message with `messageType: PAYMENT` (no priority header) → Routes to `PAYMENT` topic
- Message with `messageType: ACCOUNT_TRANSACTION` (no priority header) → Routes to `ACCOUNT_TRANSACTION` topic
- High-priority messages get dedicated topics for faster processing, separate consumer groups, and stricter SLAs

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
- IBM MQ 9.2.3+ environment with Streaming Queues configured
- IBM MQ Source Connector deployed in Confluent Cloud
- MQ messages with routing properties set (e.g., `messageType`)
- Kafka topics created (or auto-creation enabled)

If you need help setting up the MQ connector, see the [Confluent IBM MQ Source Connector documentation](https://docs.confluent.io/kafka-connectors/ibm-mq-source/current/overview.html).

## Important: MQ Message Properties

For routing to work, your MQ messages **must** include the properties you're routing on.

**Example: Setting MQ properties in Java:**

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
2. MQ automatically duplicates the message (with all properties) to `KAFKA.AGGREGATION.QUEUE` (via `STREAMQ` configuration)
3. IBM MQ Source Connector reads from `KAFKA.AGGREGATION.QUEUE` and converts MQ properties to Kafka headers
4. SMTs use the Kafka headers for routing

## Error Handling

### Dead Letter Queue (DLQ)

Configure DLQ to catch routing errors:

```json
"kafka.topic": "mq-unrouted-dlq",

"errors.tolerance": "all",
"errors.deadletterqueue.topic.name": "mq-error-dlq",
"errors.deadletterqueue.context.headers.enable": "true"
```

- **kafka.topic** (fallback): Messages without routing headers go here
- **errors.deadletterqueue.topic.name**: Messages that fail processing go here

## Troubleshooting

### Messages going to wrong topic
- Check the header value matches topic name exactly (case-sensitive)
- Verify MQ property is being set correctly
- Check connector logs for routing decisions

### Messages going to default topic instead of being routed
- Verify header exists on the message (check in Confluent Cloud UI)
- Confirm MQ message has the property set
- Check connector config has `mq.message.body.jms: "true"`

### Topic not found errors
- Enable auto topic creation, OR
- Pre-create all expected topics, OR
- Use DLQ to catch messages for non-existent topics

## Resources

- [Confluent IBM MQ Source Connector](https://docs.confluent.io/kafka-connectors/ibm-mq-source/current/overview.html)
- [RegexRouter SMT Documentation](https://docs.confluent.io/platform/current/connect/transforms/regexrouter.html)
- [Kafka Connect Transformations](https://docs.confluent.io/platform/current/connect/transforms/overview.html)
- [Kafka Connect Predicates](https://docs.confluent.io/platform/current/connect/transforms/predicates.html)

## Contributing

This repository contains reference examples. Feel free to adapt these patterns to your specific use case.

## License

See [LICENSE](LICENSE) file for details.
