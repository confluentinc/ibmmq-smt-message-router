# Test Data Publisher

This directory contains sample banking messages and a Python script to publish them to IBM MQ with proper routing metadata.

## Overview

The `publish-to-mq.py` script:
1. ✅ Loads sample banking message payloads (JSON)
2. ✅ Sets MQ message properties for routing (`messageType`, `priority`, etc.)
3. ✅ Publishes messages to the IBM MQ Gateway Queue
4. ✅ MQ properties automatically become Kafka headers in the connector

## Quick Start

### 1. Install Dependencies

```bash
cd test-data

# Install Python dependencies
pip install -r requirements.txt
```

**Note:** `pymqi` requires IBM MQ client libraries. See [Installation](#installing-pymqi) section below.

### 2. Configure MQ Connection

```bash
# Copy the template
cp mq-config.properties.example mq-config.properties

# Edit with your MQ details
nano mq-config.properties
```

Update these fields:
```properties
mq.hostname=your-mq-server.example.com
mq.port=1414
mq.queue.manager=QM1
mq.channel=SYSTEM.DEF.SVRCONN
mq.queue.name=GATEWAY.QUEUE
mq.username=your-username
mq.password=your-password
```

**Important:** Add `mq-config.properties` to `.gitignore` to avoid committing credentials.

### 3. Publish Test Messages

```bash
# Publish one message of each type (default)
python publish-to-mq.py

# Publish 10 messages of each type
python publish-to-mq.py --count 10

# Publish only domestic payments
python publish-to-mq.py --type PAYMENT_DOMESTIC --count 5

# Use custom config file
python publish-to-mq.py --config my-custom-config.properties
```

## Sample Messages

### Available Message Types

| File | Message Type | Target Topic | MQ Properties |
|------|--------------|--------------|---------------|
| `payment-domestic.json` | `PAYMENT_DOMESTIC` | `PAYMENT_DOMESTIC` | priority: NORMAL, businessUnit: RETAIL_BANKING |
| `payment-international.json` | `PAYMENT_INTERNATIONAL` | `PAYMENT_INTERNATIONAL` | priority: HIGH, businessUnit: CORPORATE_BANKING |
| `account-transaction.json` | `ACCOUNT_TRANSACTION` | `ACCOUNT_TRANSACTION` | priority: NORMAL, businessUnit: RETAIL_BANKING |
| `fraud-alert.json` | `FRAUD_ALERT` | `FRAUD_ALERT` | priority: CRITICAL, businessUnit: SECURITY |

### Message Structure

Each message includes:
- **Payload (JSON)**: Banking transaction data
- **MQ Properties**: Routing metadata that becomes Kafka headers
  - `messageType`: Determines target Kafka topic
  - `sourceQueue`: Original MQ queue name
  - `priority`: Message priority level
  - `businessUnit`: Department/division code
  - `JMSCorrelationID`: Unique transaction correlation ID

### Example: Domestic Payment

**Payload (`payment-domestic.json`):**
```json
{
  "transactionId": "TXN-DOM-20260819-001",
  "amount": 1500.00,
  "currency": "USD",
  "fromAccount": "9876543210",
  "toAccount": "1234567890",
  "description": "Rent payment"
}
```

**MQ Properties (set by publisher script):**
```
messageType: PAYMENT_DOMESTIC
sourceQueue: PAYMENT.DOMESTIC.QUEUE
priority: NORMAL
businessUnit: RETAIL_BANKING
JMSCorrelationID: DEMO-PAYMENT_DOMESTIC-1724078400-1
```

**Result in Kafka:**
- **Topic**: `PAYMENT_DOMESTIC` (routed by messageType)
- **Key**: `DEMO-PAYMENT_DOMESTIC-1724078400-1` (from correlation ID)
- **Headers**: All MQ properties converted to Kafka headers
- **Value**: JSON payload + `kafkaIngestTime` field added by SMT

## Script Usage

### Command-Line Options

```
python publish-to-mq.py [OPTIONS]

Options:
  --config FILE       MQ config file (default: mq-config.properties)
  --type TYPE         Publish only specific message type
  --count N           Number of messages per type (default: 1)
  --delay SECONDS     Delay between messages (default: 0.1)
  -h, --help          Show help message
```

### Examples

**Publish one of each type:**
```bash
python publish-to-mq.py
```

**Load test with 100 messages:**
```bash
python publish-to-mq.py --count 100 --delay 0.01
```

**Publish only fraud alerts:**
```bash
python publish-to-mq.py --type FRAUD_ALERT --count 20
```

**Use different config:**
```bash
python publish-to-mq.py --config prod-mq-config.properties
```

### Output

```
Connecting to MQ...
  Host: mq.example.com
  Port: 1414
  Queue Manager: QM1
  Channel: SYSTEM.DEF.SVRCONN
✅ Connected to MQ successfully

✅ Opened queue: GATEWAY.QUEUE

============================================================
Publishing Messages
============================================================

📤 Publishing 1 x PAYMENT_DOMESTIC
  ✅ [1/1] Published with correlation ID: DEMO-PAYMENT_DOMESTIC-1724078400-1

📤 Publishing 1 x PAYMENT_INTERNATIONAL
  ✅ [1/1] Published with correlation ID: DEMO-PAYMENT_INTERNATIONAL-1724078401-1

...

============================================================
Summary
============================================================
✅ Successfully published: 4

Messages are in queue: GATEWAY.QUEUE
The IBM MQ Source Connector will:
  1. Read messages from the Gateway Queue
  2. Extract MQ properties as Kafka headers
  3. Route to topics based on messageType header
```

## Verification

### 1. Check MQ Queue Depth

Verify messages are in the Gateway Queue:

```mqsc
DISPLAY QSTATUS(GATEWAY.QUEUE) CURDEPTH
```

Or via MQ Console UI.

### 2. Monitor Connector

In Confluent Cloud:
1. Go to Connectors → `mq-gateway-banking-source`
2. Check **Metrics** tab for throughput
3. Should see messages/sec increasing

### 3. View Messages in Kafka Topics

In Confluent Cloud UI:
1. Go to **Topics**
2. Select `PAYMENT_DOMESTIC` (or other topic)
3. Click **Messages** tab
4. Set offset to "Latest"
5. Click Play to start consuming

**Verify:**
- ✅ Messages appear in correct topics
- ✅ Headers include `messageType`, `sourceQueue`, etc.
- ✅ Payload matches what you published
- ✅ `kafkaIngestTime` field added by connector SMT

## Installing pymqi

### macOS

```bash
# Install IBM MQ client (required by pymqi)
brew install ibm-mq

# Install pymqi
pip install pymqi
```

### Linux (Ubuntu/Debian)

```bash
# Download IBM MQ client from IBM website
# https://www.ibm.com/support/pages/downloading-ibm-mq-redistributable-clients

# Extract and set environment variables
export MQSERVER='CHANNEL/TCP/HOST(PORT)'
export LD_LIBRARY_PATH=/opt/mqm/lib64:$LD_LIBRARY_PATH

# Install pymqi
pip install pymqi
```

### Docker Alternative

If you can't install MQ client libraries locally:

```dockerfile
FROM python:3.11-slim

# Install IBM MQ client
RUN apt-get update && apt-get install -y wget
RUN wget https://public.dhe.ibm.com/ibmdl/export/pub/software/websphere/messaging/mqdev/redist/9.3.0.0-IBM-MQC-Redist-LinuxX64.tar.gz
RUN tar -xzf 9.3.0.0-IBM-MQC-Redist-LinuxX64.tar.gz -C /opt

ENV LD_LIBRARY_PATH=/opt/mqm/lib64
ENV C_INCLUDE_PATH=/opt/mqm/inc

RUN pip install pymqi

COPY . /app
WORKDIR /app

CMD ["python", "publish-to-mq.py"]
```

## Customizing Messages

### Modify Existing Messages

Edit the JSON files in `sample-messages/`:

```bash
nano sample-messages/payment-domestic.json
```

Update fields like amounts, account numbers, descriptions, etc.

### Add New Message Types

1. Create new JSON file in `sample-messages/`
2. Update `MESSAGE_TYPES` dict in `publish-to-mq.py`:
   ```python
   MESSAGE_TYPES = {
       "YOUR_NEW_TYPE": "sample-messages/your-new-type.json"
   }
   ```
3. Add metadata:
   ```python
   MESSAGE_METADATA = {
       "YOUR_NEW_TYPE": {
           "priority": "NORMAL",
           "businessUnit": "YOUR_UNIT",
           "sourceQueue": "YOUR.SOURCE.QUEUE"
       }
   }
   ```
4. Create corresponding Kafka topic in Confluent Cloud

### Modify Routing Metadata

Edit `MESSAGE_METADATA` in `publish-to-mq.py` to change:
- Priority levels
- Business unit codes
- Source queue names

## Troubleshooting

### Error: "pymqi module not installed"

```bash
pip install pymqi
```

If it fails, ensure IBM MQ client libraries are installed (see [Installing pymqi](#installing-pymqi)).

### Error: "Config file not found"

```bash
cp mq-config.properties.example mq-config.properties
# Edit with your MQ details
nano mq-config.properties
```

### Error: "MQRC_NOT_AUTHORIZED (2035)"

- Verify username/password are correct
- Check MQ user has CONNECT permission on queue manager
- Check MQ user has GET permission on gateway queue
- Review MQ channel authentication (CHLAUTH) rules

### Error: "MQRC_UNKNOWN_OBJECT_NAME (2085)"

- Verify queue name is spelled correctly (case-sensitive)
- Check queue exists in MQ:
  ```mqsc
  DISPLAY QUEUE(GATEWAY.QUEUE)
  ```

### Error: "Connection refused"

- Verify MQ hostname and port
- Check MQ listener is running:
  ```mqsc
  DISPLAY LISTENER(*)
  ```
- Check firewall rules allow connection on MQ port

### Messages published but not appearing in Kafka

1. Check connector status (should be "Running")
2. Verify connector is reading from correct queue
3. Check `mq-unrouted-dlq` topic for unrouted messages
4. Check `mq-error-dlq` topic for processing errors
5. Verify MQ queue depth is decreasing (messages being consumed)

## Advanced Usage

### Batch Publishing

For large-scale testing:

```bash
# Publish 1000 messages quickly
for i in {1..10}; do
  python publish-to-mq.py --count 100 --delay 0 &
done
wait
```

### Continuous Publishing

For sustained load testing:

```bash
# Publish messages every 5 seconds indefinitely
while true; do
  python publish-to-mq.py --count 1
  sleep 5
done
```

### Publishing from CSV/Database

Modify `publish-to-mq.py` to read from external sources:

```python
import csv

with open('transactions.csv') as f:
    reader = csv.DictReader(f)
    for row in reader:
        payload = {
            "transactionId": row['id'],
            "amount": float(row['amount']),
            # ... map CSV columns to payload fields
        }
        publish_message(queue, "PAYMENT_DOMESTIC", payload, metadata, row['id'])
```

## Security Best Practices

1. **Never commit credentials**
   - Add `mq-config.properties` to `.gitignore`
   - Use environment variables or secrets manager

2. **Use TLS/SSL in production**
   - Configure SSL cipher suite in config
   - Use certificates for authentication

3. **Least privilege access**
   - MQ user should only have GET on Gateway Queue
   - No PUT/BROWSE/ADMIN permissions needed

4. **Rotate credentials regularly**
   - Update MQ passwords periodically
   - Use service accounts, not personal accounts

## Next Steps

After publishing test messages:

1. ✅ **Verify in Confluent Cloud UI** (Topics → Messages)
2. ✅ **Check routing worked correctly** (messages in right topics)
3. ✅ **Set up downstream consumers** (apps, ksqlDB, etc.)
4. ✅ **Test error scenarios** (missing headers, malformed messages)
5. ✅ **Monitor connector metrics** (throughput, lag, errors)

## Resources

- [pymqi Documentation](https://dsuch.github.io/pymqi/)
- [IBM MQ Documentation](https://www.ibm.com/docs/en/ibm-mq)
- [Confluent IBM MQ Source Connector](https://docs.confluent.io/kafka-connectors/ibm-mq-source/current/overview.html)
