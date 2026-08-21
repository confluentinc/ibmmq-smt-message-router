# Architecture Deep Dive

> 📊 **Visual Diagrams:** For interactive visual representations of this architecture, see the [diagrams/](diagrams/) folder. Includes complete architecture diagram, sequence flow, and routing logic decision trees.

## IBM MQ Gateway Queue Pattern

### The Pattern

The Gateway Queue pattern is an IBM-recommended design for consolidating multiple message sources into a single consumption point. This reduces infrastructure overhead and simplifies integration.

**Traditional Approach (Not Recommended):**
```
ORDER.QUEUE ──────► MQ Source Connector 1 ──► orders-topic
PAYMENT.QUEUE ────► MQ Source Connector 2 ──► payments-topic
FRAUD.QUEUE ──────► MQ Source Connector 3 ──► fraud-topic
```
❌ Multiple connectors = more resources, more management overhead

**Gateway Queue Pattern (Recommended):**
```
ORDER.QUEUE ──┐
PAYMENT.QUEUE ├─► GATEWAY.QUEUE ──► Single MQ Source Connector ──► Multiple Kafka Topics
FRAUD.QUEUE ──┘                      (SMT routing)
```
✅ Single connector, dynamic routing based on message metadata

### How Messages Flow

#### 1. Banking Application Publishes to MQ

```java
// Core banking system publishes domestic payment
QueueConnection qConn = qConnFactory.createQueueConnection();
QueueSession session = qConn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
TextMessage message = session.createTextMessage(paymentJsonPayload);

// Set MQ properties for routing
message.setStringProperty("messageType", "PAYMENT_DOMESTIC");
message.setStringProperty("sourceQueue", "PAYMENT.DOMESTIC.QUEUE");
message.setStringProperty("priority", "HIGH");
message.setStringProperty("businessUnit", "RETAIL_BANKING");
message.setJMSCorrelationID("TXN-12345-67890");

Queue gatewayQueue = session.createQueue("GATEWAY.QUEUE");
QueueSender sender = session.createSender(gatewayQueue);
sender.send(message);
```

#### 2. MQ Queue Manager Routes to Gateway Queue

MQ can use several mechanisms to route to the gateway queue:
- **Alias Queues**: Define source queues as aliases pointing to GATEWAY.QUEUE
- **Forwarding Rules**: Use MQSC commands to forward messages
- **Application Logic**: Applications write directly to GATEWAY.QUEUE

#### 3. IBM MQ Source Connector Consumes from Gateway Queue

The connector:
1. Connects to `GATEWAY.QUEUE`
2. Reads each message
3. **Automatically converts MQ properties → Kafka headers**
4. Applies SMT chain to transform/route message
5. Produces to Kafka topic determined by SMT

#### 4. SMT Routing Logic

```json
{
  "transforms": "addMetadata,routeByMessageType",
  
  "transforms.addMetadata.type": "org.apache.kafka.connect.transforms.InsertField$Value",
  "transforms.addMetadata.timestamp.field": "kafkaIngestTime",
  
  "transforms.routeByMessageType.type": "org.apache.kafka.connect.transforms.RegexRouter",
  "transforms.routeByMessageType.regex": ".*",
  "transforms.routeByMessageType.replacement": "${header:messageType}"
}
```

**What happens:**
1. `addMetadata` SMT adds `kafkaIngestTime` field to message payload
2. `routeByMessageType` SMT reads the Kafka header `messageType` (originally from MQ property)
3. Router uses header value as the target topic name
4. Message with `messageType: PAYMENT_DOMESTIC` → `PAYMENT_DOMESTIC` topic

## Message Structure

### MQ Message Format

**MQ Message Properties (become Kafka headers):**
```
messageType: "PAYMENT_DOMESTIC"
sourceQueue: "PAYMENT.DOMESTIC.QUEUE"
priority: "HIGH"
businessUnit: "RETAIL_BANKING"
JMSCorrelationID: "TXN-12345-67890"
JMSMessageID: "ID:414d51..."
```

**MQ Message Payload:**
```json
{
  "transactionId": "TXN-12345",
  "amount": 1500.00,
  "currency": "USD",
  "fromAccount": "9876543210",
  "toAccount": "1234567890",
  "timestamp": "2026-08-19T14:30:00Z"
}
```

### Kafka Record After Connector Processing

**Kafka Headers:**
```
messageType: PAYMENT_DOMESTIC
sourceQueue: PAYMENT.DOMESTIC.QUEUE
priority: HIGH
businessUnit: RETAIL_BANKING
JMSCorrelationID: TXN-12345-67890
JMSMessageID: ID:414d51...
```

**Kafka Key:** 
```
TXN-12345-67890  (from JMSCorrelationID)
```

**Kafka Value:**
```json
{
  "transactionId": "TXN-12345",
  "amount": 1500.00,
  "currency": "USD",
  "fromAccount": "9876543210",
  "toAccount": "1234567890",
  "timestamp": "2026-08-19T14:30:00Z",
  "kafkaIngestTime": "2026-08-19T14:30:05.123Z"
}
```

**Kafka Topic:**
```
PAYMENT_DOMESTIC  (routed by messageType header)
```

## SMT Chain Breakdown

### Transform 1: InsertField (Metadata Enrichment)

```json
{
  "transforms.addMetadata.type": "org.apache.kafka.connect.transforms.InsertField$Value",
  "transforms.addMetadata.timestamp.field": "kafkaIngestTime",
  "transforms.addMetadata.static.field": "sourceSystem",
  "transforms.addMetadata.static.value": "CORE_BANKING_MQ"
}
```

**Purpose:** Add audit trail metadata to every message
- `kafkaIngestTime`: When connector ingested the message
- `sourceSystem`: Identifies source as "CORE_BANKING_MQ"

### Transform 2: RegexRouter (Topic Routing)

```json
{
  "transforms.routeByMessageType.type": "org.apache.kafka.connect.transforms.RegexRouter",
  "transforms.routeByMessageType.regex": ".*",
  "transforms.routeByMessageType.replacement": "${header:messageType}"
}
```

**Purpose:** Route message to topic based on `messageType` header

**How it works:**
- `regex: ".*"` matches any default topic name (we use `mq-unrouted-dlq` as default)
- `replacement: "${header:messageType}"` replaces with value from header
- If header exists: message goes to topic named by header value
- If header missing: message goes to default topic (`mq-unrouted-dlq`)

## Alternative Routing Patterns

### Pattern 1: Topic Prefix Routing

**Use case:** Namespace all MQ-sourced topics with a prefix

```json
{
  "transforms.routeByMessageType.replacement": "mq-${header:messageType}"
}
```

**Result:**
- `PAYMENT_DOMESTIC` → `mq-PAYMENT_DOMESTIC`
- `FRAUD_ALERT` → `mq-FRAUD_ALERT`

### Pattern 2: Lowercase Topic Names

**Use case:** Follow Kafka naming conventions (lowercase-with-dashes)

First, need to transform the header value to lowercase. Requires custom SMT or pre-processing.

**Alternative:** Set lowercase messageType in MQ:
```java
message.setStringProperty("messageType", "payment-domestic");
```

### Pattern 3: Multi-Dimensional Routing

**Use case:** Route by business unit AND message type

```json
{
  "transforms.routeByMessageType.replacement": "${header:businessUnit}-${header:messageType}"
}
```

**Result:**
- `businessUnit: RETAIL`, `messageType: PAYMENT_DOMESTIC` → `RETAIL-PAYMENT_DOMESTIC`
- `businessUnit: CORPORATE`, `messageType: PAYMENT_DOMESTIC` → `CORPORATE-PAYMENT_DOMESTIC`

### Pattern 4: Conditional Routing with Predicates

**Use case:** Route high-priority messages to separate topics

```json
{
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
}
```

## Error Handling & Dead Letter Queues

### DLQ Configuration

```json
{
  "errors.tolerance": "all",
  "errors.deadletterqueue.topic.name": "mq-error-dlq",
  "errors.deadletterqueue.context.headers.enable": "true"
}
```

### Error Scenarios

| Scenario | Behavior | Destination |
|----------|----------|-------------|
| Message has valid `messageType` header | Normal processing | Routed to `${messageType}` topic |
| Message missing `messageType` header | Routes to default topic | `mq-unrouted-dlq` |
| Connector fails to parse message | Error tolerance catches it | `mq-error-dlq` |
| Target topic doesn't exist | Auto-create (if enabled) or error | `mq-error-dlq` if auto-create disabled |
| Serialization error | Error tolerance catches it | `mq-error-dlq` |

### DLQ Message Headers

When a message goes to the error DLQ, additional headers are added:

```
__connect.errors.topic: original-topic-name
__connect.errors.partition: 0
__connect.errors.offset: 12345
__connect.errors.connector.name: mq-gateway-source
__connect.errors.task.id: 0
__connect.errors.stage: VALUE_CONVERTER
__connect.errors.exception.class.name: org.apache.kafka.connect.errors.DataException
__connect.errors.exception.message: Failed to deserialize value...
__connect.errors.exception.stacktrace: [full stack trace]
```

## Scalability Considerations

### Single Connector Limits

- **Tasks:** IBM MQ Source Connector typically runs with `tasks.max: 1` (single consumer per queue)
- **Throughput:** Depends on message size and network latency
- **Typical capacity:** 1,000 - 10,000 msgs/sec per connector

### Scaling Strategies

#### Horizontal Scaling (Multiple Gateway Queues)

```
GATEWAY.QUEUE.1 ──► MQ Source Connector 1 ──┐
GATEWAY.QUEUE.2 ──► MQ Source Connector 2 ──┼─► Kafka Topics
GATEWAY.QUEUE.3 ──► MQ Source Connector 3 ──┘
```

Use MQ routing rules to distribute messages across gateway queues.

#### Vertical Scaling (Kafka Topic Partitions)

```json
{
  "confluent kafka topic create payments-domestic --partitions 10"
}
```

More partitions = higher parallelism for downstream consumers.

## Monitoring & Observability

### Key Metrics to Monitor

**Connector Metrics:**
- `source-record-poll-rate`: Messages/sec from MQ
- `source-record-write-rate`: Messages/sec to Kafka
- Task status (running/failed)

**MQ Queue Metrics:**
- Queue depth (should be near zero if connector is keeping up)
- Oldest message age
- Enqueue/dequeue rates

**Kafka Topic Metrics:**
- Messages in/sec per topic
- Consumer lag (for downstream consumers)

### Alerting Recommendations

1. **Connector task failed** → Page on-call
2. **MQ queue depth > 1000** → Warning alert
3. **Zero messages in 5 minutes** → Investigate (expected traffic?)
4. **Messages in `mq-error-dlq`** → Review and fix

## Security Considerations

### TLS/SSL Configuration

```json
{
  "mq.tls.truststore.location": "/path/to/truststore.jks",
  "mq.tls.truststore.password": "${file:/path/to/truststore-password.txt}",
  "mq.ssl.cipher.suite": "TLS_RSA_WITH_AES_128_CBC_SHA256"
}
```

### Authentication

```json
{
  "mq.username": "mquser",
  "mq.password": "${secret:mq-password}"
}
```

Always use Confluent Cloud secrets management for credentials.

### Authorization

MQ user needs:
- **CONNECT** permission on queue manager
- **GET** permission on `GATEWAY.QUEUE`
- **BROWSE** permission (optional, for monitoring)

## Performance Tuning

### Batch Size

```json
{
  "batch.size": "500",
  "mq.batch.size": "100"
}
```

Larger batches = higher throughput, but more memory usage.

### Polling Interval

```json
{
  "mq.polling.timeout": "30000"
}
```

Timeout for polling MQ queue (milliseconds).

### Message Converter

```json
{
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false"
}
```

Use schema registry for better performance with large messages:
```json
{
  "value.converter": "io.confluent.connect.avro.AvroConverter",
  "value.converter.schema.registry.url": "https://..."
}
```

## Testing Strategy

### Unit Testing (Message Publisher)

Test that MQ properties are set correctly:
```python
# Verify messageType property is set
assert message.getStringProperty('messageType') == 'PAYMENT_DOMESTIC'
```

### Integration Testing (End-to-End)

1. Publish message to MQ with known `messageType`
2. Wait for connector to process
3. Consume from target Kafka topic
4. Verify message routed correctly and payload intact

### Load Testing

Use JMeter or custom script to:
1. Publish 10,000 msgs/sec to MQ Gateway Queue
2. Monitor connector lag
3. Verify all messages reach Kafka
4. Check for any errors in DLQ

## Disaster Recovery

### Connector Failure

- Connector stops → Messages accumulate in MQ
- MQ queue persistence ensures no data loss
- Restart connector → Processes from where it left off

### Kafka Cluster Failure

- Enable Confluent Cloud multi-zone deployment
- Messages remain in MQ until Kafka recovers
- No data loss due to MQ persistence

### MQ Failure

- Connector will retry connection
- Configure retry policy:
  ```json
  {
    "errors.retry.timeout": "300000",
    "errors.retry.delay.max.ms": "60000"
  }
  ```
