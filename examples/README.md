# SMT Routing Configuration Examples

This directory contains complete connector configuration examples demonstrating different SMT routing patterns for IBM MQ Source Connector.

## Examples Overview

Each JSON file is a complete connector configuration with focus on the `transforms` section. Replace the `${VARIABLE}` placeholders with your actual MQ connection details.

### 1. basic-routing.json

**Pattern:** Simple header-based routing

**Configuration:**
```json
"transforms": "route",
"transforms.route.replacement": "${header:messageType}"
```

**Routing Logic:**
- Reads `messageType` header from Kafka message (originally MQ property)
- Routes message to topic matching the header value

**Example:**
```
messageType: PAYMENT_DOMESTIC  → Topic: PAYMENT_DOMESTIC
messageType: FRAUD_ALERT       → Topic: FRAUD_ALERT
No messageType header          → Topic: mq-unrouted-dlq (fallback)
```

**Use When:**
- You have a single routing dimension
- Topic names match header values exactly
- Simplest routing scenario

---

### 2. routing-with-prefix.json

**Pattern:** Namespaced topic routing

**Configuration:**
```json
"transforms": "route",
"transforms.route.replacement": "banking-${header:messageType}"
```

**Routing Logic:**
- Adds `banking-` prefix to all routed topics
- Useful for topic namespace organization

**Example:**
```
messageType: PAYMENT_DOMESTIC  → Topic: banking-PAYMENT_DOMESTIC
messageType: FRAUD_ALERT       → Topic: banking-FRAUD_ALERT
```

**Use When:**
- You want to namespace topics (e.g., by system, domain, or team)
- Avoid topic name conflicts with other systems
- Maintain consistent topic naming conventions

---

### 3. multi-dimensional-routing.json

**Pattern:** Route by multiple headers

**Configuration:**
```json
"transforms": "route",
"transforms.route.replacement": "${header:businessUnit}-${header:messageType}"
```

**Routing Logic:**
- Combines two headers to create topic name
- Creates topics like `RETAIL-PAYMENT`, `CORPORATE-PAYMENT`

**Example:**
```
businessUnit: RETAIL    + messageType: PAYMENT  → Topic: RETAIL-PAYMENT
businessUnit: CORPORATE + messageType: PAYMENT  → Topic: CORPORATE-PAYMENT
businessUnit: RETAIL    + messageType: FRAUD    → Topic: RETAIL-FRAUD
```

**Use When:**
- Need to segregate messages by multiple dimensions
- Different departments/regions need separate topics
- Want to partition data by business context

---

### 4. routing-with-metadata.json

**Pattern:** Enrich messages before routing

**Configuration:**
```json
"transforms": "addTimestamp,addSource,route",
"transforms.addTimestamp.type": "InsertField$Value",
"transforms.addSource.type": "InsertField$Value",
"transforms.route.type": "RegexRouter"
```

**Routing Logic:**
- **Step 1:** Add `kafkaIngestTime` field to message payload
- **Step 2:** Add `sourceSystem: CORE_BANKING_MQ` field
- **Step 3:** Route based on `messageType` header

**Example:**
```json
// Original message
{"transactionId": "123", "amount": 1000}

// After enrichment
{
  "transactionId": "123",
  "amount": 1000,
  "kafkaIngestTime": "2026-08-21T10:30:00Z",
  "sourceSystem": "CORE_BANKING_MQ"
}
```

**Use When:**
- Need audit trail timestamps
- Want to track message source/lineage
- Add metadata for downstream processing

---

### 5. conditional-routing.json

**Pattern:** Different routing rules based on conditions

**Configuration:**
```json
"transforms": "routeHighPriority,routeNormal",
"predicates": "isHighPriority",

"predicates.isHighPriority.type": "HasHeaderKey",
"predicates.isHighPriority.name": "priority",

"transforms.routeHighPriority.replacement": "${header:messageType}-priority",
"transforms.routeHighPriority.predicate": "isHighPriority",

"transforms.routeNormal.replacement": "${header:messageType}",
"transforms.routeNormal.negate": "true"
```

**Routing Logic:**
- If message has `priority` header → Route to `{messageType}-priority` topic
- If message lacks `priority` header → Route to `{messageType}` topic

**Example:**
```
priority: HIGH + messageType: PAYMENT  → Topic: PAYMENT-priority
messageType: PAYMENT (no priority)     → Topic: PAYMENT
```

**Use When:**
- Need separate processing for urgent/high-priority messages
- Want to apply different retention policies by priority
- SLA requirements differ by message type

---

## Using These Examples

### Step 1: Choose Your Pattern

Select the example that matches your routing requirements.

### Step 2: Copy Configuration

Copy the entire JSON configuration from the example file.

### Step 3: Replace Placeholders

Update these values with your actual MQ connection details:
```
${MQ_HOSTNAME}        → your-mq-server.example.com
${MQ_PORT}            → 1414
${MQ_QUEUE_MANAGER}   → QM1
${MQ_CHANNEL}         → SYSTEM.DEF.SVRCONN
${MQ_USERNAME}        → your-username
${MQ_PASSWORD}        → your-password (or use secrets: ${secret:mq-password})
```

### Step 4: Deploy Connector

Deploy via Confluent Cloud UI or CLI:

**Via UI:**
1. Go to Connectors → Add Connector → IBM MQ Source
2. Paste the configuration
3. Launch

**Via CLI:**
```bash
confluent connect cluster create --config-file basic-routing.json
```

### Step 5: Verify Routing

1. Send test messages to MQ with appropriate headers
2. Check Confluent Cloud Topics to see messages routed correctly

## Important Notes

### MQ Message Properties Required

For routing to work, MQ messages MUST have the properties set:

```java
// Java example
TextMessage msg = session.createTextMessage(payload);
msg.setStringProperty("messageType", "PAYMENT_DOMESTIC");
msg.setStringProperty("priority", "HIGH");
msg.setStringProperty("businessUnit", "RETAIL");
```

### Connector Must Parse MQ Properties

Ensure your connector configuration includes:
```json
"mq.message.body.jms": "true"
```

This tells the connector to parse MQ/JMS properties and convert them to Kafka headers.

### Topic Creation

Topics must exist before routing (unless auto-creation is enabled):

```bash
# Create topics manually if needed
confluent kafka topic create PAYMENT_DOMESTIC --partitions 3
confluent kafka topic create FRAUD_ALERT --partitions 3
```

Or enable auto-creation in your Kafka cluster settings.

## Combining Patterns

You can mix and match these patterns. For example:

**Enrichment + Multi-dimensional routing:**
```json
"transforms": "addTimestamp,route",
"transforms.addTimestamp.type": "InsertField$Value",
"transforms.addTimestamp.timestamp.field": "ingestedAt",
"transforms.route.replacement": "${header:businessUnit}-${header:messageType}"
```

**Prefix + Conditional routing:**
```json
"transforms": "routeHighPriority,routeNormal",
"transforms.routeHighPriority.replacement": "banking-${header:messageType}-priority",
"transforms.routeNormal.replacement": "banking-${header:messageType}"
```

## Troubleshooting

**Messages not routing:**
- Check MQ messages have the required properties set
- Verify `mq.message.body.jms: "true"` in connector config
- Look for messages in fallback topic (`kafka.topic` config)

**Headers missing:**
- Confirm MQ properties are set correctly
- Check connector is using DefaultRecordBuilder
- Verify JMS message format

**Topic not found:**
- Pre-create topics, or enable auto-creation
- Check topic name matches exactly (case-sensitive)
- Review connector logs for errors

## Resources

- [RegexRouter SMT](https://docs.confluent.io/platform/current/connect/transforms/regexrouter.html)
- [InsertField SMT](https://docs.confluent.io/platform/current/connect/transforms/insertfield.html)
- [Kafka Connect Predicates](https://docs.confluent.io/platform/current/connect/transforms/predicates.html)
