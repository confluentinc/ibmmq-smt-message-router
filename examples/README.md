# SMT Routing Configuration Examples

This directory contains complete connector configuration examples demonstrating header-based topic routing patterns for JMS Source Connectors.

## Supported JMS Providers

Examples are available for multiple JMS providers:

- **[ibm-mq/](ibm-mq/)** - IBM MQ Source Connector configurations
- **[activemq/](activemq/)** - ActiveMQ Source Connector configurations

**Note:** SMT configuration (`transforms` section) is identical across providers. Only connector-specific properties (connection details, credentials) differ.

## Available Routing Patterns

Each directory contains 5 routing patterns:

| Pattern | Description | Key SMT |
|---------|-------------|---------|
| **basic-routing.json** | Route by messageType header | ExtractTopic$Header |
| **routing-with-prefix.json** | Add namespace prefix (e.g., `banking-PAYMENT`) | ExtractTopic$Header + RegexRouter |
| **multi-dimensional-routing.json** | Route by combined routing key | ExtractTopic$Header |
| **routing-with-metadata.json** | Enrich payload then route | InsertField + ExtractTopic$Header |
| **conditional-routing.json** | Route based on header presence | Predicates + ExtractTopic$Header |

## Quick Start

### 1. Choose Your Provider

Navigate to the directory for your JMS provider:
- IBM MQ: `ibm-mq/`
- ActiveMQ: `activemq/`

### 2. Select a Routing Pattern

Choose the JSON file that matches your routing requirements.

### 3. Replace Placeholders

Update connection details:

**IBM MQ:**
```
${MQ_HOSTNAME}      → your-mq-server.example.com
${MQ_PORT}          → 1414
${MQ_QUEUE_MANAGER} → QM1
${MQ_CHANNEL}       → SYSTEM.DEF.SVRCONN
${MQ_USERNAME}      → your-username
${MQ_PASSWORD}      → your-password
```

**ActiveMQ:**
```
${ACTIVEMQ_HOST}     → your-activemq-server.example.com
${ACTIVEMQ_USERNAME} → your-username
${ACTIVEMQ_PASSWORD} → your-password
```

### 4. Deploy Connector

Deploy via Confluent Cloud:

**UI:**
1. Go to Connectors → Add Connector → (IBM MQ Source or ActiveMQ Source)
2. Paste the configuration
3. Launch

**CLI:**
```bash
confluent connect cluster create --config-file ibm-mq/basic-routing.json
```

### 5. Verify Routing

1. Send test JMS messages with appropriate properties (e.g., `messageType: PAYMENT`)
2. Check Confluent Cloud Topics to see messages routed to correct topics

## Important Requirements

### JMS Message Properties Must Be Set

For routing to work, JMS messages **must** include the routing properties:

```java
// Java example
TextMessage message = session.createTextMessage(payload);
message.setStringProperty("messageType", "PAYMENT_DOMESTIC");
message.setStringProperty("priority", "HIGH");

// Application publishes to normal queue
Queue appQueue = session.createQueue("PAYMENT.APP.QUEUE");
sender.send(appQueue, message);
```

### JMS Properties → Kafka Headers

The JMS Source Connector automatically converts JMS properties to Kafka headers, which the SMTs then use for routing.

**For IBM MQ Connector:**
```json
"mq.message.body.jms": "true"
```

This ensures MQ properties are parsed and converted to Kafka headers.

### Topic Creation

Ensure destination topics exist before routing:

```bash
# Create topics manually
confluent kafka topic create PAYMENT_DOMESTIC --partitions 3
confluent kafka topic create FRAUD_ALERT --partitions 3
```

Or enable auto topic creation in your Kafka cluster settings.

## Pattern Details

For detailed explanations of each routing pattern, see the main [README](../README.md) which includes:
- How each pattern works
- Sample message routing examples
- Use cases for each pattern
- SMT configuration chains

## Key SMT: ExtractTopic$Header

The primary SMT used for header-based routing is **ExtractTopic$Header**:

```json
"transforms": "route",
"transforms.route.type": "io.confluent.connect.transforms.ExtractTopic$Header",
"transforms.route.field": "messageType",
"transforms.route.skip.missing.or.null": "true"
```

This SMT:
- Reads the specified header value (`messageType`)
- Uses that value as the destination topic name
- Falls back to `kafka.topic` if header is missing

**Note:** The standard `RegexRouter` SMT does NOT support `${header:xxx}` interpolation syntax. Use `ExtractTopic$Header` for header-based routing.

## Troubleshooting

**Messages not routing to expected topics:**
- Verify JMS properties are set on source messages
- Check connector config has JMS property parsing enabled
- Look for messages in fallback/DLQ topic

**Headers missing from Kafka records:**
- Confirm JMS properties are set correctly on source messages
- For IBM MQ: verify `mq.message.body.jms: "true"` is set
- Check connector logs for property conversion errors

**Topic not found errors:**
- Pre-create all expected destination topics, or
- Enable auto topic creation in Kafka cluster settings

## Resources

**Connector Documentation:**
- [IBM MQ Source Connector](https://docs.confluent.io/kafka-connectors/ibm-mq-source/current/overview.html)
- [ActiveMQ Source Connector](https://docs.confluent.io/kafka-connectors/activemq-source/current/overview.html)

**SMT Documentation:**
- [ExtractTopic SMT](https://docs.confluent.io/platform/current/connect/transforms/extracttopic.html)
- [RegexRouter SMT](https://docs.confluent.io/platform/current/connect/transforms/regexrouter.html)
- [Kafka Connect Transformations](https://docs.confluent.io/platform/current/connect/transforms/overview.html)
- [Kafka Connect Predicates](https://docs.confluent.io/platform/current/connect/transforms/predicates.html)
