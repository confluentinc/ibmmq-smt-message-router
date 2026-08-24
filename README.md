# IBM MQ SMT Message Router Examples

Single Message Transformation (SMT) examples for routing IBM MQ messages to different Kafka topics based on message headers.

## Overview

This repository provides **SMT configuration examples** for the IBM MQ Source Connector to route messages from a single MQ Gateway Queue to multiple Kafka topics based on message metadata.

### Use Case

When using the **IBM MQ Gateway Queue pattern** (consolidating multiple MQ queues into one), you need to route messages to different Kafka topics based on their content or metadata. This is achieved using Confluent's `RegexRouter` SMT.

### How It Works

1. MQ messages include properties (e.g., `messageType`) that indicate message category
2. IBM MQ Source Connector automatically converts MQ properties → Kafka headers
3. `RegexRouter` SMT reads the Kafka header and routes to the appropriate topic
4. **No custom code required** - purely configuration-based routing

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

## Prerequisites

This assumes you already have:
- ✅ IBM MQ environment with Gateway Queue configured
- ✅ IBM MQ Source Connector deployed in Confluent Cloud
- ✅ MQ messages with routing properties set (e.g., `messageType`)
- ✅ Kafka topics created (or auto-creation enabled)

If you need help setting up the MQ connector, see the [Confluent IBM MQ Source Connector documentation](https://docs.confluent.io/kafka-connectors/ibm-mq-source/current/overview.html).

## Important: MQ Message Properties

For routing to work, your MQ messages **must** include the properties you're routing on.

**Example: Setting MQ properties in Java:**

```java
TextMessage message = session.createTextMessage(payload);
message.setStringProperty("messageType", "PAYMENT_DOMESTIC");
message.setStringProperty("priority", "HIGH");
message.setStringProperty("businessUnit", "RETAIL");

Queue gatewayQueue = session.createQueue("GATEWAY.QUEUE");
sender.send(gatewayQueue, message);
```

The IBM MQ Source Connector automatically converts these MQ properties to Kafka headers, which the SMT can then use for routing.

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
- ✅ Check the header value matches topic name exactly (case-sensitive)
- ✅ Verify MQ property is being set correctly
- ✅ Check connector logs for routing decisions

### Messages going to default topic instead of being routed
- ✅ Verify header exists on the message (check in Confluent Cloud UI)
- ✅ Confirm MQ message has the property set
- ✅ Check connector config has `mq.message.body.jms: "true"`

### Topic not found errors
- ✅ Enable auto topic creation, OR
- ✅ Pre-create all expected topics, OR
- ✅ Use DLQ to catch messages for non-existent topics

## Resources

- [Confluent IBM MQ Source Connector](https://docs.confluent.io/kafka-connectors/ibm-mq-source/current/overview.html)
- [RegexRouter SMT Documentation](https://docs.confluent.io/platform/current/connect/transforms/regexrouter.html)
- [Kafka Connect Transformations](https://docs.confluent.io/platform/current/connect/transforms/overview.html)
- [Kafka Connect Predicates](https://docs.confluent.io/platform/current/connect/transforms/predicates.html)

## Contributing

This repository contains reference examples. Feel free to adapt these patterns to your specific use case.

## License

See [LICENSE](LICENSE) file for details.
