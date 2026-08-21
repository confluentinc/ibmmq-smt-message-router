# Confluent Cloud MQ Gateway Banking Demo

A complete proof-of-concept demonstrating IBM MQ Gateway Queue pattern with SMT-based message routing in Confluent Cloud for banking use cases.

## Overview

This demo shows how to integrate a core banking system that uses IBM MQ with Confluent Cloud, leveraging the **Gateway Queue pattern** (IBM recommended design) to consolidate multiple message types into a single queue, then route them to different Kafka topics based on message metadata using Single Message Transformations (SMTs).

### Architecture

```
┌──────────────────────────────────────────────────────┐
│         Core Banking System (IBM MQ)                 │
│                                                      │
│  ORDER.QUEUE  PAYMENT.QUEUE  FRAUD.QUEUE            │
│       │              │              │                │
│       └──────────────┼──────────────┘                │
│                      ▼                                │
│              GATEWAY.QUEUE                           │
│         (Single aggregated queue)                    │
└──────────────────────┬───────────────────────────────┘
                       │
                       │ Each message has MQ properties:
                       │ - messageType (for routing)
                       │ - sourceQueue
                       │ - priority
                       │
                       ▼
┌──────────────────────────────────────────────────────┐
│    Confluent Cloud - IBM MQ Source Connector         │
│                                                      │
│    SMT Chain: RegexRouter routes by messageType     │
│    (MQ properties automatically become Kafka headers)│
└──────────────────────┬───────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────┐
│         Confluent Cloud Kafka Cluster                │
│                                                      │
│  payments-domestic     payments-international        │
│  account-transactions  fraud-alerts                  │
└──────────────────────────────────────────────────────┘
```

> 📊 **Interactive Diagrams:** See the [diagrams/](diagrams/) folder for detailed Mermaid diagrams including complete architecture, sequence flows, and routing logic. These diagrams render automatically on GitHub and can be exported to PNG/SVG for presentations.

### Message Types

| Message Type | Target Kafka Topic | Description |
|--------------|-------------------|-------------|
| `PAYMENT_DOMESTIC` | `payments-domestic` | Domestic bank transfers |
| `PAYMENT_INTERNATIONAL` | `payments-international` | International wire transfers |
| `ACCOUNT_TRANSACTION` | `account-transactions` | Account debits/credits |
| `FRAUD_ALERT` | `fraud-alerts` | Fraud detection alerts |

## Prerequisites

See [prerequisites.md](prerequisites.md) for detailed requirements:

- ✅ IBM MQ environment with Gateway Queue configured
- ✅ Confluent Cloud account with Connect cluster
- ✅ Python 3.8+ (for test data publisher)
- ✅ Confluent CLI installed and authenticated

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/your-org/confluent-mq-gateway-banking-demo.git
cd confluent-mq-gateway-banking-demo
```

### 2. Create Kafka Topics

```bash
cd confluent-cloud/topics
./create-topics.sh
```

This creates the following topics:
- `payments-domestic`
- `payments-international`
- `account-transactions`
- `fraud-alerts`
- `mq-unrouted-dlq` (dead letter queue for unrouted messages)
- `mq-error-dlq` (dead letter queue for connector errors)

### 3. Deploy the IBM MQ Source Connector

See [confluent-cloud/connector/README.md](confluent-cloud/connector/README.md) for detailed steps.

**Option A: Via Confluent Cloud UI**
1. Navigate to your Confluent Cloud cluster → Connectors
2. Click "Add connector" → Select "IBM MQ Source"
3. Copy/paste configuration from `confluent-cloud/connector/mq-gateway-connector.json`
4. Update connection details for your MQ environment
5. Launch the connector

**Option B: Via Confluent CLI**
```bash
cd confluent-cloud/connector

# Edit mq-gateway-connector.json with your MQ details
# Then deploy:
confluent connect cluster create \
  --config-file mq-gateway-connector.json \
  --cluster <your-kafka-cluster-id>
```

### 4. Publish Test Banking Messages

```bash
cd test-data

# Configure MQ connection
cp mq-config.properties.example mq-config.properties
# Edit mq-config.properties with your MQ connection details

# Install dependencies
pip install -r requirements.txt

# Publish sample messages
python publish-to-mq.py
```

### 5. Verify Routing in Confluent Cloud UI

1. Navigate to **Topics** in Confluent Cloud UI
2. Select `payments-domestic` topic → **Messages** tab
3. You should see messages with `messageType: PAYMENT_DOMESTIC`
4. Check other topics similarly
5. Verify messages are routed correctly based on their `messageType` property

**What to verify:**
- ✅ Messages appear in correct topics based on messageType
- ✅ Kafka message headers include MQ properties (messageType, sourceQueue, priority)
- ✅ Message payloads are intact
- ✅ Timestamps are preserved

## Repository Structure

```
confluent-mq-gateway-banking-demo/
├── README.md                    # This file
├── ARCHITECTURE.md              # Detailed architecture explanation
├── prerequisites.md             # Requirements checklist
│
├── diagrams/                    # Mermaid architecture diagrams
│   ├── architecture.mmd                  # Complete end-to-end architecture
│   ├── simplified-architecture.mmd       # High-level overview
│   ├── message-flow-sequence.mmd         # Step-by-step message flow
│   ├── smt-routing-logic.mmd             # SMT routing decision tree
│   └── README.md                         # Diagram usage guide
│
├── confluent-cloud/
│   ├── connector/
│   │   ├── mq-gateway-connector.json     # Connector configuration
│   │   └── README.md                     # Deployment guide
│   │
│   └── topics/
│       ├── create-topics.sh              # Topic creation script
│       ├── topic-configs.json            # Topic configuration reference
│       └── README.md                     # Topic setup guide
│
└── test-data/
    ├── sample-messages/                  # Sample JSON files
    │   ├── payment-domestic.json
    │   ├── payment-international.json
    │   ├── account-transaction.json
    │   └── fraud-alert.json
    │
    ├── publish-to-mq.py                  # Test data publisher
    ├── mq-config.properties.example      # MQ connection template
    ├── requirements.txt                  # Python dependencies
    └── README.md                         # Publisher usage guide
```

## How It Works

### MQ Message Properties → Kafka Headers

When applications publish messages to MQ, they set JMS/MQ properties:

```java
// Example: Publishing from core banking app
TextMessage message = session.createTextMessage(paymentJson);
message.setStringProperty("messageType", "PAYMENT_DOMESTIC");
message.setStringProperty("sourceQueue", "PAYMENT.DOMESTIC.QUEUE");
message.setStringProperty("priority", "HIGH");
jmsProducer.send(gatewayQueue, message);
```

The IBM MQ Source Connector **automatically converts** these MQ properties to Kafka headers.

### SMT-Based Routing

The connector's `RegexRouter` SMT reads the `messageType` Kafka header and routes to the corresponding topic:

```json
{
  "transforms": "routeByMessageType",
  "transforms.routeByMessageType.type": "org.apache.kafka.connect.transforms.RegexRouter",
  "transforms.routeByMessageType.regex": ".*",
  "transforms.routeByMessageType.replacement": "${header:messageType}"
}
```

### Fallback & Error Handling

- Messages **without** a `messageType` header → `mq-unrouted-dlq` topic
- Messages that **fail processing** → `mq-error-dlq` topic

## Customization

### Adding New Message Types

1. Add new topic:
   ```bash
   confluent kafka topic create new-message-type --partitions 3
   ```

2. Update your MQ applications to set `messageType` property:
   ```java
   message.setStringProperty("messageType", "new-message-type");
   ```

3. Messages will automatically route to the new topic (no connector restart needed!)

### Alternative Routing Strategies

See `ARCHITECTURE.md` for other routing patterns:
- Route by business unit: `${header:businessUnit}-${header:messageType}`
- Route by priority: Separate topics for high-priority messages
- Route by source queue: Preserve original MQ queue structure

## Monitoring

Monitor connector health in Confluent Cloud:
1. Navigate to **Connectors** → Select your MQ connector
2. Check **Status** tab for running tasks
3. View **Metrics** tab for throughput and lag
4. Check **Errors** for any failures

## Troubleshooting

### Messages not appearing in topics
- ✅ Check connector status (should be "Running")
- ✅ Verify MQ connection details are correct
- ✅ Check that MQ messages have `messageType` property set
- ✅ Look in `mq-unrouted-dlq` for messages without routing headers

### Routing to wrong topic
- ✅ Verify `messageType` property value matches target topic name exactly
- ✅ Check connector configuration for correct `replacement` pattern
- ✅ View message headers in Confluent Cloud UI to see actual values

### Connector tasks failing
- ✅ Check MQ queue manager is accessible from Confluent Cloud
- ✅ Verify credentials are correct
- ✅ Check `mq-error-dlq` topic for failed messages with error context

## Next Steps

- 📖 Read [ARCHITECTURE.md](ARCHITECTURE.md) for deeper technical details
- 🔧 Explore alternative SMT configurations for different routing patterns
- 🚀 Integrate with downstream consumers (ksqlDB, Stream Processing, etc.)
- 📊 Set up monitoring and alerting for production use

## Resources

- [IBM MQ Source Connector Documentation](https://docs.confluent.io/kafka-connectors/ibm-mq-source/current/overview.html)
- [Single Message Transformations (SMT) Guide](https://docs.confluent.io/platform/current/connect/transforms/overview.html)
- [Confluent Cloud Documentation](https://docs.confluent.io/cloud/current/overview.html)

## License

Apache 2.0
