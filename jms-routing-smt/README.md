# JMS Message Routing - Detailed Guide

Route JMS messages to different Kafka topics based on message properties.

**Tested:** IBM MQ, ActiveMQ Classic, ActiveMQ Artemis

Confluent's built-in `ExtractTopic$Value` SMT supports `field.format=JSON_PATH`, so no custom code is needed. See [JSONPATH_DISCOVERY.md](../JSONPATH_DISCOVERY.md) for the test results.

## Problem

JMS properties are nested in the connector's JSON output:

```json
{
  "properties": {
    "messageType": {
      "string": "PAYMENT"
    }
  }
}
```

Most SMTs can't extract `properties.messageType.string` directly.

## Which to Use?

| | **Flink** | **JSON_PATH SMT** | **Custom SMT** |
|-|-----------|-------------------|----------------|
| Input topic retained (audit, replay) | Yes | No | No |
| Delivery | Exactly-once on Confluent Cloud | Connector-dependent | Connector-dependent |
| Latency | 1-5 sec | Sub-second | Sub-second |
| Setup | ~15 min | ~5 min | ~30 min |
| Custom code | No | No | Yes |
| Aggregations | Yes | No | No |

- Need an audit trail, replay, several routing strategies on one input, or aggregations? → **[Flink](flink-routing/)**
- Want minimal storage and direct routing? → **JSON_PATH SMT**
- **Custom SMT**: superseded by JSON_PATH. Kept for reference only.

## JSON_PATH SMT

Add to your connector config:

```json
{
  "transforms": "routeByJsonPath",
  "transforms.routeByJsonPath.type": "io.confluent.connect.transforms.ExtractTopic$Value",
  "transforms.routeByJsonPath.field": "$[\"properties\"][\"messageType\"][\"string\"]",
  "transforms.routeByJsonPath.field.format": "JSON_PATH",
  "transforms.routeByJsonPath.skip.missing.or.null": "true"
}
```

The SMT extracts `properties.messageType.string` and uses it as the topic name, so a `messageType` of `PAYMENT` goes to the topic `PAYMENT`. With `skip.missing.or.null=true`, records without the property stay on the connector's `kafka.topic`.

See the [main README](../README.md#configuration) for full connector configs.

## Flink

```
MQ → Connector → ibm.mq.input → Flink → payment-topic, transfer-topic, notification-topic
```

Run the SQL from [flink-routing/routing.sql](flink-routing/routing.sql). [Full guide →](flink-routing/)

## Custom SMT (Historical)

`JmsPropertyToHeader$Value` copies `properties.<name>.string` into a Kafka header. Combined with `ExtractTopic$Header` it routes by header value. Use JSON_PATH instead; it does the same in one transform.

### Configuration

```json
{
  "connector.class": "io.confluent.connect.ibm.mq.IbmMQSourceConnector",
  "kafka.topic": "ibm.mq.input",
  "jms.destination.name": "DEV.QUEUE.1",

  "transforms": "copyMessageType,routeByMessageType",

  "transforms.copyMessageType.type": "io.confluent.connect.transforms.JmsPropertyToHeader$Value",
  "transforms.copyMessageType.property.name": "messageType",
  "transforms.copyMessageType.header.name": "messageType",
  "transforms.copyMessageType.skip.missing": "true",

  "transforms.routeByMessageType.type": "io.confluent.connect.transforms.ExtractTopic$Header",
  "transforms.routeByMessageType.field": "messageType",
  "transforms.routeByMessageType.skip.missing.or.null": "true"
}
```

`ExtractTopic$Header` uses the whole header value as the topic name (`PAYMENT` → topic `PAYMENT`). It has no `topic.format` option, and only `field.format=PLAIN` is supported for headers.

### Parameters

**JmsPropertyToHeader:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `property.name` | String | Required | JMS property to extract (e.g. `messageType`) |
| `header.name` | String | Required | Kafka header to create |
| `skip.missing` | Boolean | `true` | If true, pass records through unchanged when the property is missing or null. If false, fail with a `DataException`. |

**ExtractTopic$Header** (Confluent-provided):

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `field` | String | Required | Name of the header to read (matches `header.name` above) |
| `skip.missing.or.null` | Boolean | `false` | Skip records where the header is missing or null |

Only the `$Value` variant exists. The transform returns a new record with a copy of the headers and leaves the input record unchanged. It handles both schema-based (`Struct`) and schemaless (`Map`) values.

### Build and test

```bash
mvn clean package   # runs the unit tests, output: target/jms-property-to-header-smt-1.0.0.jar
```

The jar is built by CI; it is no longer committed to the repository.

### Deploy to Confluent Cloud

Custom SMTs on Confluent Cloud need a custom connector plugin:

```bash
mkdir -p kafka-connect-jms-smt/lib
cp target/jms-property-to-header-smt-1.0.0.jar kafka-connect-jms-smt/lib/
cd kafka-connect-jms-smt
zip -r ../jms-property-to-header-smt-plugin.zip .
```

Upload the ZIP under **Connectors → Custom connector plugins** and reference the plugin from your connector.

### Deploy to self-managed Connect

```bash
mkdir -p /usr/local/share/kafka/plugins/jms-property-to-header-smt
cp target/jms-property-to-header-smt-1.0.0.jar /usr/local/share/kafka/plugins/jms-property-to-header-smt/
```

Set `plugin.path=/usr/local/share/kafka/plugins` in the worker configuration and restart Connect. The jar is compiled against Kafka 4.x and needs Java 11+; use a Connect runtime of the same major version.

## Testing

Send a JMS message with the property:

```java
TextMessage msg = session.createTextMessage("{\"transactionId\":\"PAY-001\"}");
msg.setStringProperty("messageType", "PAYMENT");
producer.send(msg);
```

Verify the message arrives in the `PAYMENT` topic.

## Repository layout

```
jms-routing-smt/
├── flink-routing/   # Flink SQL (Confluent Cloud) and Table API job (self-managed)
├── src/             # Custom SMT and its unit tests (historical)
└── README.md        # This file
```
