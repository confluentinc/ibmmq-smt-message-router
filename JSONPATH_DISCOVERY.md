# JSON_PATH Discovery - Simplified JMS Routing

**Date:** 2026-09-28  
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
  "transforms.routeByMessageType.header": "messageType",
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

**Test Date:** 2026-09-28  
**Connector:** ActiveMQSourceConnector_0 (lcc-12572g6)  
**Environment:** Confluent Cloud (t36181/lkc-gxm1kv)

### Configuration Tested
```json
{
  "name": "ActiveMQSourceConnector_0",
  "config": {
    "connector.class": "ActiveMQSource",
    "activemq.url": "tcp://74.234.194.75:61616",
    "activemq.username": "admin",
    "activemq.password": "admin",
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
cd /tmp && java -cp "lib/*:." SendActiveMQMessages
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

## Impact

### Documentation Updates Required
1. ✅ Main README.md - Updated to recommend JSON_PATH as primary SMT alternative
2. ✅ jms-routing-smt/README.md - Updated to show JSON_PATH as recommended, custom SMT as historical
3. ✅ Added JSON_PATH configuration examples for all JMS providers

### Recommendations Updated
**New hierarchy:**
1. **Apache Flink** (recommended for production)
   - Exactly-once semantics
   - Input topic retained for audit trail
   - Stateful operations support

2. **JSON_PATH SMT** (recommended for simple routing)
   - Single built-in SMT
   - No custom code
   - Direct routing, no input topic

3. **Custom SMT** (historical reference)
   - Superseded by JSON_PATH
   - Documentation kept for reference only

## Lessons Learned

1. **Always check latest documentation** - Confluent's SMT documentation revealed the JSON_PATH support that wasn't immediately obvious
2. **Test before building** - Could have saved time building custom SMT if we'd discovered this earlier
3. **Built-in > custom** - When built-in features exist, they're always preferable to custom code

## References

- **Confluent ExtractTopic Documentation:** https://docs.confluent.io/cloud/current/connectors/transforms/extracttopic.html
- **JSON Path Support:** field.format=JSON_PATH parameter enables nested extraction
- **Test Results:** Connector lcc-12572g6 in environment t36181

## Conclusion

The JSON_PATH discovery simplifies the JMS routing solution significantly. For users who don't need input topic retention, the JSON_PATH approach is now the recommended SMT-based solution, completely eliminating the need for custom code.

For users who need input topic retention and audit trail, Apache Flink remains the recommended approach.
