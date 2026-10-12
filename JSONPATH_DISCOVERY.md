# JSON_PATH Discovery - Simplified JMS Routing

**Tested:** September 2026  
**Discovery:** Confluent's `ExtractTopic$Value` SMT supports `field.format=JSON_PATH`, eliminating the need for custom SMTs.

## Summary

While building a custom SMT to extract nested JMS properties (`properties.messageType.string`) and route messages, we discovered that Confluent's built-in `ExtractTopic$Value` transform already supports JSON_PATH expressions for nested field extraction.

**This means:** A single built-in SMT can do what previously required a custom SMT + standard SMT combination.

## Comparison

### Old Approach (Custom SMT)
```json
{
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
- ❌ Requires custom SMT plugin upload
- ❌ Two transforms in chain
- ❌ More complex configuration
- ❌ Custom code to maintain

### New Approach (JSON_PATH)
```json
{
  "transforms": "routeByJsonPath",
  "transforms.routeByJsonPath.type": "io.confluent.connect.transforms.ExtractTopic$Value",
  "transforms.routeByJsonPath.field": "$[\"properties\"][\"messageType\"][\"string\"]",
  "transforms.routeByJsonPath.field.format": "JSON_PATH",
  "transforms.routeByJsonPath.skip.missing.or.null": "true"
}
```
- ✅ Built-in Confluent SMT (no custom code)
- ✅ Single transform
- ✅ Simpler configuration
- ✅ Works in Confluent Cloud out-of-the-box

## Test Results

**Connector:** ActiveMQ Source Connector on Confluent Cloud

### Configuration Tested
```json
{
  "name": "ActiveMQSourceConnector_0",
  "config": {
    "connector.class": "ActiveMQSource",
    "activemq.url": "tcp://your-activemq-host:61616",
    "activemq.username": "your-username",
    "activemq.password": "your-password",
    "jms.destination.name": "DEV.QUEUE",
    "kafka.topic": "activemq.jsonpath.input",
    "output.data.format": "JSON",
    "tasks.max": "1",
    "transforms": "routeByJsonPath",
    "transforms.routeByJsonPath.type": "io.confluent.connect.transforms.ExtractTopic$Value",
    "transforms.routeByJsonPath.field": "$[\"properties\"][\"messageType\"][\"string\"]",
    "transforms.routeByJsonPath.field.format": "JSON_PATH",
    "transforms.routeByJsonPath.skip.missing.or.null": "true"
  }
}
```

### Test Messages Sent
```bash
# small JMS client that sets the messageType property (see "Setting JMS Properties" in the README)
```

Sent 12 messages:
- 4 × PAYMENT messages
- 4 × TRANSFER messages
- 4 × NOTIFICATION messages

### Results
✅ **Connector Status:** RUNNING  
✅ **Topics Created:** PAYMENT, TRANSFER, NOTIFICATION  
✅ **Routing Confirmed:** Messages successfully routed to separate topics based on `messageType` property

### Message Structure
**Input (JMS message as JSON):**
```json
{
  "messageID": "MSG-1790605824441-PAYMENT",
  "timestamp": 1.790605824442E9,
  "text": "Payment of $500 processed",
  "properties": {
    "messageType": {
      "string": "PAYMENT"
    }
  }
}
```

**JSON Path Evaluation:**
- Path: `$["properties"]["messageType"]["string"]`
- Extracts: `"PAYMENT"`
- Routes to: `PAYMENT` topic

## Recommendations

1. **Apache Flink**: when you need the input topic for audit or replay, or stateful processing.
2. **JSON_PATH SMT**: simple routing with no input topic and no custom code.
3. **Custom SMT**: superseded by JSON_PATH; kept for reference only.

## References

- **Confluent ExtractTopic Documentation:** https://docs.confluent.io/cloud/current/connectors/transforms/extracttopic.html
- **JSON path support:** `field.format=JSON_PATH` enables nested extraction (`$Value` only; `$Header` supports `PLAIN` only)

## Conclusion

The JSON_PATH discovery simplifies the JMS routing solution significantly. For users who don't need input topic retention, the JSON_PATH approach is now the recommended SMT-based solution, completely eliminating the need for custom code.

For users who need input topic retention and audit trail, Apache Flink remains the recommended approach.
