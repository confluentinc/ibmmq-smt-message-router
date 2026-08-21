# IBM MQ Source Connector Deployment Guide

This guide walks through deploying the IBM MQ Source Connector to Confluent Cloud with SMT-based message routing.

## Configuration Overview

The connector configuration in `mq-gateway-connector.json` includes:

| Setting | Purpose |
|---------|---------|
| `mq.hostname`, `mq.port`, `mq.queue.manager` | MQ connection details |
| `jms.destination.name: "GATEWAY.QUEUE"` | Single aggregated queue to consume from |
| `mq.message.body.jms: "true"` | Parse JMS properties from MQ messages |
| `mq.record.builder.key.header: "JMSCorrelationID"` | Use correlation ID as Kafka record key |
| `kafka.topic: "mq-unrouted-dlq"` | Default/fallback topic for messages without routing header |
| `transforms.routeByMessageType` | SMT that routes messages by `messageType` header |
| `errors.deadletterqueue.topic.name: "mq-error-dlq"` | Error DLQ for connector failures |

## Before You Deploy

1. **Create Kafka topics** (see [../topics/README.md](../topics/README.md))
   ```bash
   cd ../topics
   ./create-topics.sh
   ```

2. **Verify MQ connectivity** from your network to ensure Confluent Cloud can reach MQ

3. **Gather MQ connection details:**
   - MQ hostname/IP
   - MQ port (usually 1414)
   - Queue Manager name
   - Channel name
   - Username and password
   - Gateway Queue name (e.g., `GATEWAY.QUEUE`)

## Deployment Options

### Option 1: Deploy via Confluent Cloud UI (Recommended for First-Time Setup)

#### Step 1: Navigate to Connectors

1. Log in to [Confluent Cloud](https://confluent.cloud)
2. Select your **Environment** and **Kafka cluster**
3. Click **Connectors** in the left sidebar
4. Click **+ Add connector**

#### Step 2: Select IBM MQ Source Connector

1. In the connector catalog, search for "IBM MQ"
2. Select **IBM MQ Source**
3. Click **Continue**

#### Step 3: Configure Connector

You can either:

**Option A: Upload JSON configuration**
1. Click **Show advanced configurations**
2. Look for JSON import option (if available)
3. Copy/paste contents of `mq-gateway-connector.json`
4. Update the placeholder values:
   - `YOUR_MQ_HOSTNAME` → your MQ host
   - `YOUR_MQ_USERNAME` → your MQ user
   - `YOUR_MQ_PASSWORD` → your MQ password
   - Update other MQ-specific settings as needed

**Option B: Fill out form manually**

**Connection Settings:**
- **Connector name:** `mq-gateway-banking-source`
- **Kafka API Key:** Select or create new
- **MQ Hostname:** Your MQ server hostname
- **MQ Port:** `1414` (or your custom port)
- **Queue Manager:** `QM1` (or your queue manager name)
- **Channel:** `SYSTEM.DEF.SVRCONN` (or your channel)
- **Queue:** `GATEWAY.QUEUE`
- **Username:** Your MQ username
- **Password:** Your MQ password

**Advanced Settings:**
- **Key converter:** `org.apache.kafka.connect.storage.StringConverter`
- **Value converter:** `org.apache.kafka.connect.json.JsonConverter`
- **Schemas enable:** `false`

**Transformations (SMT):**

Add these transforms in order:

1. **Transform 1: InsertField**
   - Type: `org.apache.kafka.connect.transforms.InsertField$Value`
   - Timestamp field: `kafkaIngestTime`
   - Static field: `sourceSystem`
   - Static value: `CORE_BANKING_MQ`

2. **Transform 2: RegexRouter**
   - Type: `org.apache.kafka.connect.transforms.RegexRouter`
   - Regex: `.*`
   - Replacement: `${header:messageType}`

**Error Handling:**
- **Error tolerance:** `all`
- **Dead letter queue topic:** `mq-error-dlq`
- **Context headers enable:** `true`

#### Step 4: Review and Launch

1. Review all settings
2. Click **Continue** or **Launch**
3. Wait for connector to provision (1-2 minutes)

#### Step 5: Verify Connector Status

1. Go to **Connectors** → `mq-gateway-banking-source`
2. Check **Status** tab:
   - Should show **Running** with 1 task
3. Check **Settings** tab to verify configuration
4. Monitor **Metrics** tab for activity

---

### Option 2: Deploy via Confluent CLI

#### Step 1: Authenticate

```bash
confluent login
confluent environment use <env-id>
confluent kafka cluster use <cluster-id>
```

#### Step 2: Edit Configuration

```bash
cd confluent-cloud/connector

# Copy template
cp mq-gateway-connector.json mq-gateway-connector-local.json

# Edit with your MQ details
nano mq-gateway-connector-local.json
# or use your preferred editor
```

**Update these fields:**
```json
{
  "mq.hostname": "mq.yourbank.com",
  "mq.username": "mqapp",
  "mq.password": "securepassword",
  "mq.queue.manager": "BANKQM1",
  "mq.channel": "CLOUD.APP.SVRCONN"
}
```

#### Step 3: Deploy Connector

```bash
confluent connect cluster create \
  --config-file mq-gateway-connector-local.json
```

#### Step 4: Monitor Deployment

```bash
# List connectors
confluent connect cluster list

# Describe specific connector
confluent connect cluster describe <connector-id>

# View connector status
confluent connect cluster status <connector-id>
```

---

## Verify Deployment

### 1. Check Connector Status

**Via UI:**
- Connectors → `mq-gateway-banking-source` → Status tab
- Should show **Running** (green indicator)
- Task status: **1/1 running**

**Via CLI:**
```bash
confluent connect cluster list
# Should show your connector with status "RUNNING"
```

### 2. Check Connector Logs (if issues)

**Via UI:**
- Connectors → `mq-gateway-banking-source` → Logs tab
- Look for connection errors or authentication issues

**Via CLI:**
```bash
confluent connect cluster describe <connector-id> --output json | jq .
```

### 3. Publish Test Message to MQ

From your MQ environment (or use the test publisher in this repo):

```bash
cd ../../test-data
python publish-to-mq.py --count 1
```

### 4. Verify Message in Kafka

**Via Confluent Cloud UI:**

1. Go to **Topics** in the left sidebar
2. Click on `PAYMENT_DOMESTIC` topic
3. Click **Messages** tab
4. Set offset to **Earliest** or **Latest**
5. Click **Play** to start consuming

**What to verify:**
- ✅ Message appears in the correct topic
- ✅ Message payload is intact
- ✅ Headers include `messageType`, `sourceQueue`, etc.
- ✅ `kafkaIngestTime` field added to payload

**Via CLI:**
```bash
confluent kafka topic consume PAYMENT_DOMESTIC --from-beginning --max-messages 1
```

---

## Common Issues & Troubleshooting

### Issue: Connector Status is "FAILED"

**Check logs for error messages:**

Common errors:

1. **"MQRC_NOT_AUTHORIZED" (2035)**
   - Fix: Verify MQ user has CONNECT and GET permissions
   - Fix: Check channel authentication rules (CHLAUTH)

2. **"MQRC_UNKNOWN_OBJECT_NAME" (2085)**
   - Fix: Verify queue name spelling (case-sensitive)
   - Fix: Check queue exists: `DISPLAY QUEUE(GATEWAY.QUEUE)`

3. **"MQRC_HOST_NOT_AVAILABLE" (2538)**
   - Fix: Verify hostname and port are correct
   - Fix: Check network connectivity / firewall rules
   - Fix: Ensure Confluent Cloud can reach MQ (check egress IPs)

4. **"Connection refused"**
   - Fix: Verify MQ listener is running on the specified port
   - Fix: Check firewall allows connections from Confluent Cloud

### Issue: Messages Not Appearing in Topics

**Checklist:**

1. ✅ Is the connector status "Running"?
2. ✅ Are messages actually in the MQ Gateway Queue?
   ```mqsc
   DISPLAY QSTATUS(GATEWAY.QUEUE) CURDEPTH
   ```
3. ✅ Do messages have the `messageType` property set?
4. ✅ Does the target Kafka topic exist?
5. ✅ Check the `mq-unrouted-dlq` topic for messages without routing headers
6. ✅ Check the `mq-error-dlq` topic for processing errors

**Debug Steps:**

```bash
# Check connector metrics
confluent connect cluster describe <connector-id>

# Check if messages are in the unrouted DLQ
confluent kafka topic consume mq-unrouted-dlq --from-beginning

# Check if messages are in the error DLQ
confluent kafka topic consume mq-error-dlq --from-beginning
```

### Issue: Messages Going to Wrong Topic

**Verify routing logic:**

1. Check the `messageType` header value on the Kafka message
2. Ensure it matches the expected topic name exactly (case-sensitive)
3. Verify connector transform configuration:
   ```json
   "transforms.routeByMessageType.replacement": "${header:messageType}"
   ```

### Issue: Missing Message Headers

If Kafka messages don't have the expected headers (messageType, sourceQueue, etc.):

1. **Verify MQ properties are set** when publishing to MQ
2. **Check connector setting:**
   ```json
   "mq.message.body.jms": "true"
   ```
3. **Verify message builder:**
   ```json
   "mq.record.builder": "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder"
   ```

---

## Advanced Configuration

### Using TLS/SSL

For production deployments, enable TLS:

```json
{
  "mq.tls.truststore.location": "/var/ssl/private/truststore.jks",
  "mq.tls.truststore.password": "${secret:truststore-password}",
  "mq.ssl.cipher.suite": "TLS_RSA_WITH_AES_256_CBC_SHA256"
}
```

Upload truststore to Confluent Cloud via UI or use secret management.

### Using Confluent Cloud Secrets

Instead of plaintext passwords:

```json
{
  "mq.password": "${secret:mq-password}"
}
```

Create secret via UI:
1. Connectors → Secrets → Add Secret
2. Name: `mq-password`
3. Value: your password

### Alternative Routing Patterns

**Route with topic prefix:**
```json
{
  "transforms.routeByMessageType.replacement": "banking-${header:messageType}"
}
```

**Route by priority:**
```json
{
  "transforms.routeByMessageType.replacement": "${header:messageType}-${header:priority}"
}
```

**Conditional routing with predicates:**
See `ARCHITECTURE.md` for examples with predicates.

---

## Updating the Connector

### Via UI

1. Go to Connectors → `mq-gateway-banking-source`
2. Click **Settings** tab
3. Click **Edit configuration**
4. Make changes
5. Click **Continue** → **Launch**

### Via CLI

```bash
# Update configuration file
nano mq-gateway-connector-local.json

# Update connector
confluent connect cluster update <connector-id> --config-file mq-gateway-connector-local.json
```

**Note:** Some changes require connector restart.

---

## Deleting the Connector

### Via UI

1. Connectors → `mq-gateway-banking-source`
2. Click **Settings** tab
3. Scroll to bottom → **Delete connector**
4. Confirm deletion

### Via CLI

```bash
confluent connect cluster delete <connector-id>
```

**Important:** This does NOT delete the Kafka topics. Delete them separately if needed:
```bash
confluent kafka topic delete PAYMENT_DOMESTIC
# Repeat for other topics
```

---

## Monitoring in Production

### Key Metrics to Watch

**In Confluent Cloud UI:**
- Connector status (Running/Failed/Paused)
- Task status (should be 1/1 running)
- Throughput (records/sec)
- Errors count

**Set up alerts for:**
- Connector task failures
- Zero throughput for extended period
- Messages appearing in error DLQ

### Integration with External Monitoring

Export metrics to Datadog, Prometheus, or other monitoring systems using Confluent Cloud Metrics API:
- [Confluent Metrics API Documentation](https://docs.confluent.io/cloud/current/monitoring/metrics-api.html)

---

## Next Steps

After successful deployment:

1. ✅ **Test with sample messages** (see `../../test-data/README.md`)
2. ✅ **Verify routing** in Confluent Cloud UI
3. ✅ **Set up downstream consumers** (applications, ksqlDB, etc.)
4. ✅ **Configure monitoring and alerts**
5. ✅ **Review security settings** (TLS, secrets, least-privilege access)

For questions or issues, see the main [README.md](../../README.md) troubleshooting section.
