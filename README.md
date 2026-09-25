# JMS Message Routing to Kafka

Route JMS messages from IBM MQ or ActiveMQ to different Kafka topics based on message properties using Flink or custom Kafka Connect SMTs.

**Supported JMS Providers:**
- IBM MQ (via IBM MQ Source Connector)
- ActiveMQ Classic (via ActiveMQ Source Connector)  
- ActiveMQ Artemis (via ActiveMQ Source Connector)

## Problem

When a single MQ queue contains messages of different types (payments, transfers, notifications), you need to route them to different Kafka topics. The alternative—deploying one connector per queue per message type—raises **significant cost and scaling concerns**.

However, JMS Source Connectors store message properties in a nested JSON structure:

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
- Running separate connectors per message type is costly and doesn't scale

## Solutions

This repository provides **two tested, production-ready solutions**:

### ✅ Recommended: Flink (Tested)

Route messages using Flink SQL with exactly-once semantics and sub-5 second latency.

**Why Flink?**
- ✅ **Exactly-once processing** - No duplicates, guaranteed correctness
- ✅ **Low latency** - 1-5 seconds end-to-end (tested)
- ✅ **Messages retained in input topic** - Audit trail and re-processing
- ✅ **Native Confluent Cloud support** - Fully managed, auto-scaling
- ✅ **Stateful operations** - Aggregations, windowing, joins

**[See Flink routing solution →](jms-routing-smt/flink-routing/)**

### ✅ Alternative: Custom SMT (Tested)

Extract nested JMS properties using a **custom SMT** (`JmsPropertyToHeader`) that reads the nested JSON and adds the value as a Kafka header. Then use the **standard `ExtractTopic` SMT** (built into Confluent Cloud) to route based on that header—all at the connector level.

**Why Custom SMT?**
- ✅ **Sub-second latency** - Routing happens at connector level
- ✅ **Lower storage costs** - Messages route directly to target topics
- ✅ **Works in Confluent Cloud** - Tested and working
- ✅ **Minimal infrastructure** - No separate stream processor needed
- ✅ **Standard SMT for routing** - Uses built-in `ExtractTopic` transform

**[See custom SMT solution →](jms-routing-smt/)**

### Quick Comparison

| Feature | **Flink** | **Custom SMT** |
|---------|-----------|----------------|
| **Processing Semantics** | Exactly-once | Depends on connector |
| **Message Retention** | **Retained in input topic** | **Not retained in input topic** |
| **Latency** | 1-5 seconds (tested) | Sub-second |
| **Confluent Cloud** | Native support | Tested & working |
| **Windowing/Aggregation** | Advanced | No |
| **Infrastructure** | Managed Flink cluster | None (in-connector) |
| **Best For** | Production with audit trail | Direct routing, minimal storage |

**See [jms-routing-smt/README.md](jms-routing-smt/README.md) for detailed comparison and decision guide.**

## Architecture

### Flink Approach (Recommended)

```
┌─────────────────┐     ┌────────────────────┐
│  MQ Broker      │────▶│  MQ Connector      │
│                 │     └────────────────────┘
│ • Streaming     │              │
│   Queue (IBM)   │              ▼
│ • Network of    │     ┌────────────────────┐
│   Brokers       │     │ jms.input topic    │
│   (ActiveMQ     │     │ (messages retained)│
│   Classic)      │     └────────────────────┘
│ • Artemis Core  │              │
│   Hub (Modern   │              ▼
│   ActiveMQ)     │     ┌────────────────────┐
└─────────────────┘     │    Flink Job       │
                        │ (exactly-once,     │
                        │  1-5s latency)     │
                        │ reads and copies   │
                        └────────────────────┘
                                 │
                    ┌────────────┼────────────┐
                    ▼            ▼            ▼
              payment-topic  transfer-topic  notification-topic

Messages available in BOTH input and output topics
```

### Custom SMT Approach

```
┌─────────────────┐     ┌────────────────────┐
│  MQ Broker      │────▶│  MQ Connector      │
│                 │     │  with SMT          │
│ • Streaming     │     │  (routes directly) │
│   Queue (IBM)   │     └────────────────────┘
│ • Network of    │              │
│   Brokers       │              │
│   (ActiveMQ     │              │ (jms.input bypassed -
│   Classic)      │              │  no messages stored)
│ • Artemis Core  │              │
│   Hub (Modern   │              │
│   ActiveMQ)     │              │
└─────────────────┘              │
                    ┌────────────┼────────────┐
                    ▼            ▼            ▼
              payment-topic  transfer-topic  notification-topic

Lower storage costs, no audit trail in input topic
```

## When to Use Each Solution

### Choose Flink if:
- You need **exactly-once processing** guarantees
- You want **messages retained in input topic** for audit/re-processing
- You need **stateful operations** (aggregations, windowing, joins)
- You're processing **financial transactions** or critical data
- You have **high throughput** requirements (100K+ msgs/sec)
- You're using **Confluent Cloud** (native, fully managed support)

### Choose Custom SMT if:
- You want **minimal storage costs** (messages stored once)
- You need **sub-second latency** at connector level
- You want **direct routing** without intermediate topic
- You're running **self-managed Kafka Connect**
- **Simple routing** is sufficient (no aggregations needed)
- You don't need audit trail in input topic

**See detailed decision guide:** [jms-routing-smt/README.md](jms-routing-smt/README.md#decision-guide)

## Prerequisites

### Required Infrastructure

**MQ Broker Side:**

This solution works with any MQ queue that contains messages with JMS properties. Common scenarios:

1. **Single queue with mixed message types** - Messages of different types (PAYMENT, TRANSFER, NOTIFICATION) sent to the same queue, each with a JMS property indicating type

2. **Aggregated queue from multiple sources** - If messages come from multiple application queues, you can optionally aggregate them using:
   - **IBM MQ**: **Streaming Queues** (IBM MQ 9.2.3+) - Duplicates messages from app queues to aggregation queue
   - **ActiveMQ Classic**: **Network of Brokers** - Hub-and-spoke topology forwarding to central hub
   - **ActiveMQ Artemis**: **Artemis Core Hub** (Artemis 2.x+) - Federation-based hub for message redistribution

**Kafka Side:**
- JMS Source Connector deployed (IBM MQ or ActiveMQ connector)
- Kafka topics created (or auto-creation enabled)
- **For Flink**: Flink compute pool in Confluent Cloud
- **For Custom SMT**: Custom plugin upload capability in Confluent Cloud

### JMS Messages
- Messages must include JMS properties for routing (e.g., `messageType`)
- See [Example: Setting JMS Properties](#example-setting-jms-properties) below for code examples

## Quick Start

**See [Prerequisites](#prerequisites) above for required MQ broker configuration (Streaming Queues, Network of Brokers, or Artemis Core Hub).**

### Option 1: Flink (15-20 minutes)

1. Navigate to Flink in Confluent Cloud
2. Open SQL workspace
3. Run SQL from [jms-routing-smt/flink-routing/routing.sql](jms-routing-smt/flink-routing/routing.sql)
4. Verify 3 routing jobs running

**Result:** Exactly-once routing with 1-5 second latency, messages retained in input topic.

**[Full Flink guide →](jms-routing-smt/flink-routing/)**

### Option 2: Custom SMT (30 minutes)

1. Upload [jms-routing-smt/jms-property-to-header-smt-plugin.zip](jms-routing-smt/jms-property-to-header-smt-plugin.zip) to Confluent Cloud
2. Add two transforms to MQ connector:
   - `JmsPropertyToHeader$Value` - Extract JMS property to Kafka header
   - `ExtractTopic$Header` - Route based on header value
3. Messages route directly to target topics

**Result:** Sub-second routing at connector level, messages not retained in input topic.

**[Full SMT guide →](jms-routing-smt/)**

## Tested Configuration

Both solutions have been tested end-to-end with:
- **IBM MQ Source Connector** in Confluent Cloud
- **Messages with JMS properties** (messageType: PAYMENT, TRANSFER, NOTIFICATION)
- **Routing to multiple topics** based on messageType value
- **Confluent Cloud deployment** (fully managed)

Performance verified:
- **Flink**: 1-5 seconds end-to-end latency, exactly-once semantics
- **Custom SMT**: Sub-second latency, messages route directly to target topics

## Repository Structure

```
.
├── README.md                          # This file - overview and quick start
│
└── jms-routing-smt/                   # Main routing solutions (TESTED)
    ├── README.md                      # Detailed comparison and decision guide
    ├── src/                           # Custom SMT source code
    │   └── main/java/io/confluent/connect/transforms/
    │       └── JmsPropertyToHeader.java
    ├── jms-property-to-header-smt-plugin.zip  # Ready-to-upload plugin
    ├── pom.xml                        # Maven build for SMT
    │
    └── flink-routing/                 # Flink solution (RECOMMENDED)
        ├── README.md                  # Full Flink deployment guide
        ├── routing.sql                # Flink SQL DDL and routing jobs
        └── java-router/               # Flink Table API (Java option)
```

## Example: Setting JMS Properties

Your JMS messages must include properties for routing to work. The examples below show **how to set JMS properties in your message producer code**. When the JMS Source Connector reads these messages from the queue, it automatically converts the JMS property (e.g., `messageType`) into the nested JSON structure (`properties.messageType.string`) that the routing solutions extract from.

### IBM MQ Example

This example demonstrates setting a `messageType` JMS property on an IBM MQ message. The IBM MQ Source Connector will convert this property into the nested structure `properties.messageType.string` in Kafka.

```java
import com.ibm.mq.jms.*;
import javax.jms.*;

// Create connection and session
MQConnectionFactory cf = new MQConnectionFactory();
cf.setHostName("localhost");
cf.setPort(1414);
cf.setQueueManager("QM1");
cf.setChannel("CONFLUENT.CHL");

Connection conn = cf.createConnection("username", "password");
Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
Queue queue = session.createQueue("DEV.QUEUE.1");
MessageProducer producer = session.createProducer(queue);

// Create message with JMS property
TextMessage msg = session.createTextMessage("{\"transactionId\":\"PAY-001\",\"amount\":5000}");
msg.setStringProperty("messageType", "PAYMENT");  // This becomes properties.messageType.string

producer.send(msg);
```

### ActiveMQ Example

This example demonstrates setting a `messageType` JMS property on an ActiveMQ message. The ActiveMQ Source Connector will convert this property into the nested structure `properties.messageType.string` in Kafka.

```java
import org.apache.activemq.*;
import javax.jms.*;

// Create connection and session
ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("tcp://localhost:61616");
Connection conn = cf.createConnection();
Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
Queue queue = session.createQueue("DEV.QUEUE");
MessageProducer producer = session.createProducer(queue);

// Create message with JMS property
TextMessage msg = session.createTextMessage("{\"transactionId\":\"PAY-001\",\"amount\":5000}");
msg.setStringProperty("messageType", "PAYMENT");  // This becomes properties.messageType.string

producer.send(msg);
```

**Result after routing:**
- Message routes to `PAYMENT` topic (or `payment-topic` depending on configuration)
- Flink: Message also available in `jms.input` topic
- Custom SMT: Message only in `PAYMENT` topic (not in jms.input)

## Resources

- [IBM MQ Source Connector Documentation](https://docs.confluent.io/kafka-connectors/ibm-mq-source/current/overview.html)
- [ActiveMQ Source Connector Documentation](https://docs.confluent.io/kafka-connectors/activemq-source/current/overview.html)
- [Flink on Confluent Cloud](https://docs.confluent.io/cloud/current/flink/overview.html)
- [Kafka Connect Single Message Transformations](https://docs.confluent.io/platform/current/connect/transforms/overview.html)

## Contributing

This repository demonstrates tested, production-ready routing solutions. Contributions welcome for:
- Additional routing patterns
- Performance optimizations
- Support for other JMS providers
- Documentation improvements

## License

See [LICENSE](LICENSE) file for details.
