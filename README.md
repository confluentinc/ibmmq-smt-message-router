# IBM MQ to Kafka Message Routing

Route IBM MQ messages to different Kafka topics based on JMS properties using Apache Flink or custom Kafka Connect SMTs.

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

## Solutions

This repository provides **two tested, production-ready solutions**:

### ✅ Recommended: Apache Flink (Tested)

Route messages using Apache Flink SQL with exactly-once semantics and sub-5 second latency.

**Why Flink?**
- ✅ **Exactly-once processing** - No duplicates, guaranteed correctness
- ✅ **Low latency** - 1-5 seconds end-to-end (tested)
- ✅ **Messages retained in input topic** - Audit trail and re-processing
- ✅ **Native Confluent Cloud support** - Fully managed, auto-scaling
- ✅ **Stateful operations** - Aggregations, windowing, joins

**[See Flink routing solution →](jms-routing-smt/flink-routing/)**

### ✅ Alternative: Custom SMT (Tested)

Extract nested JMS properties using a custom Kafka Connect SMT and route at the connector level.

**Why Custom SMT?**
- ✅ **Sub-second latency** - Routing happens at connector level
- ✅ **Lower storage costs** - Messages route directly to target topics
- ✅ **Works in Confluent Cloud** - Tested and working
- ✅ **Minimal infrastructure** - No separate stream processor needed

**[See custom SMT solution →](jms-routing-smt/)**

### Quick Comparison

| Feature | **Apache Flink** | **Custom SMT** |
|---------|------------------|----------------|
| **Processing Semantics** | Exactly-once | Depends on connector |
| **Message Retention** | **Retained in input topic** | **Not retained in input topic** |
| **Latency** | 1-5 seconds (tested) | Sub-second |
| **Confluent Cloud** | Native support | Tested & working |
| **Windowing/Aggregation** | Advanced | No |
| **Infrastructure** | Managed Flink cluster | None (in-connector) |
| **Best For** | Production with audit trail | Direct routing, minimal storage |

**See [jms-routing-smt/README.md](jms-routing-smt/README.md) for detailed comparison and decision guide.**

## Architecture

### Apache Flink Approach (Recommended)

```
IBM MQ → MQ Connector → ibm.mq.input topic (messages retained)
                             ↓
                         Flink Job
                    (exactly-once, 1-5s latency)
                    reads and copies messages
                         /   |   \
                        /    |    \
               payment-topic | notification-topic
                       transfer-topic

Messages available in BOTH input and output topics
```

### Custom SMT Approach

```
IBM MQ → MQ Connector with SMT → Direct routing to target topics
                                       ↓
               payment-topic, transfer-topic, notification-topic
               (ibm.mq.input bypassed - no messages stored there)

Lower storage costs, no audit trail in input topic
```

## Quick Start

### Option 1: Apache Flink (15-20 minutes)

1. Navigate to Flink in Confluent Cloud
2. Open SQL workspace
3. Run SQL from [jms-routing-smt/flink-routing/routing.sql](jms-routing-smt/flink-routing/routing.sql)
4. Verify 3 routing jobs running

**Result:** Exactly-once routing with 1-5 second latency, messages retained in input topic.

**[Full Flink guide →](jms-routing-smt/flink-routing/)**

### Option 2: Custom SMT (30 minutes)

1. Upload [jms-routing-smt/jms-property-to-header-smt-plugin.zip](jms-routing-smt/jms-property-to-header-smt-plugin.zip) to Confluent Cloud
2. Add two transforms to IBM MQ connector:
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
├── jms-routing-smt/                   # Main routing solutions (TESTED)
│   ├── README.md                      # Detailed comparison and decision guide
│   ├── src/                           # Custom SMT source code
│   │   └── main/java/io/confluent/connect/transforms/
│   │       └── JmsPropertyToHeader.java
│   ├── jms-property-to-header-smt-plugin.zip  # Ready-to-upload plugin
│   ├── pom.xml                        # Maven build for SMT
│   │
│   └── flink-routing/                 # Apache Flink solution (RECOMMENDED)
│       ├── README.md                  # Full Flink deployment guide
│       ├── routing.sql                # Flink SQL DDL and routing jobs
│       └── java-router/               # Flink Table API (Java option)
│
└── examples/                          # Legacy simple routing examples
    ├── ibm-mq/                        # Basic SMT patterns for IBM MQ
    └── activemq/                      # Basic SMT patterns for ActiveMQ
```

**Primary focus:** Solutions in `jms-routing-smt/` directory (tested with nested JMS properties)

## When to Use Each Solution

### Choose Apache Flink if:
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

- IBM MQ Source Connector deployed in Confluent Cloud
- IBM MQ messages with JMS properties set (e.g., `messageType`)
- Kafka topics created (or auto-creation enabled)
- For Flink: Flink compute pool in Confluent Cloud
- For Custom SMT: Custom plugin upload capability

## Example: Setting JMS Properties

Your JMS messages must include properties for routing to work:

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

**Result after routing:**
- Message routes to `PAYMENT` topic (or `payment-topic` depending on configuration)
- Flink: Message also available in `ibm.mq.input` topic
- Custom SMT: Message only in `PAYMENT` topic (not in ibm.mq.input)

## Legacy Examples

The [examples/](examples/) directory contains basic SMT routing patterns that work when JMS properties are already available as Kafka headers. These are simpler patterns that don't require custom SMTs but cannot handle the nested structure from the IBM MQ connector.

**For nested JMS property routing (IBM MQ connector), use the solutions in `jms-routing-smt/` instead.**

## Resources

**Connectors:**
- [IBM MQ Source Connector Documentation](https://docs.confluent.io/kafka-connectors/ibm-mq-source/current/overview.html)
- [Apache Flink on Confluent Cloud](https://docs.confluent.io/cloud/current/flink/overview.html)

**Technologies:**
- [Apache Flink](https://flink.apache.org/)
- [Kafka Connect Single Message Transformations](https://docs.confluent.io/platform/current/connect/transforms/overview.html)

## Contributing

This repository demonstrates tested, production-ready routing solutions. Contributions welcome for:
- Additional routing patterns
- Performance optimizations
- Support for other JMS providers
- Documentation improvements

## License

See [LICENSE](LICENSE) file for details.
