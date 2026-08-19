# Kafka Topics for MQ Gateway Banking Demo

This directory contains scripts and configuration for creating the required Kafka topics in Confluent Cloud.

## Topics Overview

### Banking Message Topics

| Topic Name | Purpose | MQ Message Type | Expected Volume |
|------------|---------|-----------------|-----------------|
| `PAYMENT_DOMESTIC` | Domestic transfers | `PAYMENT_DOMESTIC` | High |
| `PAYMENT_INTERNATIONAL` | International wires | `PAYMENT_INTERNATIONAL` | Medium |
| `ACCOUNT_TRANSACTION` | Account debits/credits | `ACCOUNT_TRANSACTION` | Very High |
| `FRAUD_ALERT` | Fraud detection alerts | `FRAUD_ALERT` | Low |

### Operational Topics

| Topic Name | Purpose | When Used |
|------------|---------|-----------|
| `mq-unrouted-dlq` | Messages without routing headers | When MQ message lacks `messageType` property |
| `mq-error-dlq` | Connector processing errors | When connector fails to process message |

## Quick Start

### Create All Topics

```bash
cd confluent-cloud/topics
chmod +x create-topics.sh
./create-topics.sh
```

The script will:
1. ✅ Verify Confluent CLI is installed and authenticated
2. ✅ Create all 6 topics with proper configuration
3. ✅ Skip topics that already exist
4. ✅ Display summary of created topics

### Prerequisites

Before running the script:

1. **Install Confluent CLI**
   ```bash
   # macOS
   brew install confluentinc/tap/cli
   
   # Linux
   curl -sL --http1.1 https://cnfl.io/cli | sh -s -- latest
   ```

2. **Authenticate to Confluent Cloud**
   ```bash
   confluent login
   ```

3. **Select Environment and Cluster**
   ```bash
   # List environments
   confluent environment list
   
   # Use your environment
   confluent environment use <env-id>
   
   # List Kafka clusters
   confluent kafka cluster list
   
   # Use your cluster
   confluent kafka cluster use <cluster-id>
   ```

## Topic Configuration Details

All topics are created with:
- **Partitions:** 3 (allows up to 3 parallel consumers)
- **Replication Factor:** 3 (Confluent Cloud default, managed automatically)
- **Retention:** 7 days (604800000 ms) for most topics
- **Cleanup Policy:** delete (time-based retention)

See [topic-configs.json](topic-configs.json) for complete configuration reference.

### Why These Settings?

**3 Partitions:**
- Enables parallel processing by multiple consumers
- Good balance for demo/PoC workloads
- Can scale up to 10+ partitions for production

**7-Day Retention:**
- Sufficient for replay and debugging
- Balances storage costs with data availability
- Fraud alerts use 30 days for compliance

**Delete Cleanup Policy:**
- Messages automatically expire after retention period
- Simpler than compaction for this use case
- Appropriate for event streams (vs entity state)

## Manual Topic Creation

If you prefer to create topics manually via UI:

### Via Confluent Cloud UI

1. Navigate to your Kafka cluster
2. Click **Topics** in left sidebar
3. Click **+ Add topic**
4. Enter topic name (e.g., `PAYMENT_DOMESTIC`)
5. Set partitions: **3**
6. Click **Create with defaults** or **Customize settings**:
   - Retention time: **7 days**
   - Cleanup policy: **delete**
   - Compression: **producer**
7. Repeat for all 6 topics

### Via Confluent CLI (Individual Topics)

```bash
# Banking topics
confluent kafka topic create PAYMENT_DOMESTIC --partitions 3
confluent kafka topic create PAYMENT_INTERNATIONAL --partitions 3
confluent kafka topic create ACCOUNT_TRANSACTION --partitions 3
confluent kafka topic create FRAUD_ALERT --partitions 3

# Operational topics
confluent kafka topic create mq-unrouted-dlq --partitions 1
confluent kafka topic create mq-error-dlq --partitions 1
```

## Verifying Topics

### List All Topics

```bash
confluent kafka topic list
```

Look for:
- PAYMENT_DOMESTIC
- PAYMENT_INTERNATIONAL
- ACCOUNT_TRANSACTION
- FRAUD_ALERT
- mq-unrouted-dlq
- mq-error-dlq

### Describe a Specific Topic

```bash
confluent kafka topic describe PAYMENT_DOMESTIC
```

Expected output:
```
Topic: PAYMENT_DOMESTIC
Partition Count: 3
Replication Factor: 3
Configs:
  retention.ms=604800000
  cleanup.policy=delete
```

### View Topic Configuration

```bash
confluent kafka topic describe PAYMENT_DOMESTIC --output json | jq .
```

## Viewing Messages in Confluent Cloud UI

After the connector is running and messages are flowing:

1. Go to **Topics** in Confluent Cloud
2. Click on a topic (e.g., `PAYMENT_DOMESTIC`)
3. Click **Messages** tab
4. Set offset to **Earliest** or **Latest**
5. Click **Play** button to start consuming

**What to look for:**
- ✅ Messages with banking data in the value
- ✅ Headers showing `messageType`, `sourceQueue`, etc.
- ✅ Proper routing (PAYMENT_DOMESTIC messages in PAYMENT_DOMESTIC topic)
- ✅ Timestamp and partition information

### Filtering Messages

In the Confluent Cloud UI, you can filter messages by:
- **Key**: Filter by transaction ID or correlation ID
- **Headers**: Filter by messageType or other headers
- **Value**: Search within message payload (JSON)

## Consuming Messages via CLI

For debugging or verification:

```bash
# Consume from beginning
confluent kafka topic consume PAYMENT_DOMESTIC --from-beginning

# Consume latest messages
confluent kafka topic consume PAYMENT_DOMESTIC

# Consume specific number of messages
confluent kafka topic consume PAYMENT_DOMESTIC --max-messages 10

# Consume with headers visible
confluent kafka topic consume PAYMENT_DOMESTIC --print-key --print-headers --from-beginning
```

## Monitoring Topic Health

### Check Topic Lag

For topics with consumers:

```bash
confluent kafka consumer group list
confluent kafka consumer group describe <consumer-group-id>
```

### Check Message Count (Approximate)

```bash
# Not directly supported in Confluent Cloud CLI
# Use UI: Topics → [topic] → Overview → Messages/sec chart
```

### Monitor Error DLQs

Regularly check the DLQ topics for issues:

```bash
# Check unrouted messages
confluent kafka topic consume mq-unrouted-dlq --from-beginning --max-messages 10

# Check error messages
confluent kafka topic consume mq-error-dlq --from-beginning --max-messages 10
```

**If messages appear in DLQs:**
- `mq-unrouted-dlq` → MQ messages missing `messageType` property
- `mq-error-dlq` → Connector or serialization errors (check headers for error details)

## Updating Topic Configuration

### Via UI

1. Topics → Select topic → **Configuration** tab
2. Click **Edit settings**
3. Modify retention, compression, etc.
4. Save changes

### Via CLI

```bash
confluent kafka topic update PAYMENT_DOMESTIC \
  --config retention.ms=1209600000  # Change to 14 days
```

## Deleting Topics

### Via UI

1. Topics → Select topic
2. Click **Settings** or gear icon
3. Scroll to bottom → **Delete topic**
4. Confirm deletion

### Via CLI

```bash
confluent kafka topic delete PAYMENT_DOMESTIC
```

**⚠️ Warning:** Topic deletion is permanent and cannot be undone. All messages in the topic will be lost.

## Scaling Topics

### Increase Partitions

If you need higher throughput:

```bash
confluent kafka topic update PAYMENT_DOMESTIC --partitions 10
```

**Important:** You can only increase partitions, not decrease them.

### Adjust Retention

For compliance requirements:

```bash
# 30 days
confluent kafka topic update FRAUD_ALERT --config retention.ms=2592000000

# Infinite retention (not recommended for cost reasons)
confluent kafka topic update FRAUD_ALERT --config retention.ms=-1
```

## Troubleshooting

### Script fails with "Confluent CLI not found"

Install Confluent CLI:
```bash
brew install confluentinc/tap/cli  # macOS
# Or see: https://docs.confluent.io/confluent-cli/current/install.html
```

### Script fails with "Not authenticated"

```bash
confluent login
confluent environment use <env-id>
confluent kafka cluster use <cluster-id>
```

### Topic already exists

The script will skip existing topics automatically. To recreate a topic:
1. Delete it first: `confluent kafka topic delete <topic-name>`
2. Re-run the script

### Cannot see messages in topic

Possible causes:
1. ✅ Connector not deployed yet → Deploy connector first
2. ✅ Connector not running → Check connector status
3. ✅ No messages published to MQ → Publish test messages
4. ✅ Wrong offset selected → Try "Earliest" offset in UI
5. ✅ Messages routed to different topic → Check mq-unrouted-dlq

## Next Steps

After creating topics:

1. ✅ **Deploy the IBM MQ Source Connector** (see [../connector/README.md](../connector/README.md))
2. ✅ **Publish test messages to MQ** (see [../../test-data/README.md](../../test-data/README.md))
3. ✅ **Verify routing in UI** as described above
4. ✅ **Set up downstream consumers** for your use case

## Resources

- [Confluent Cloud Topics Documentation](https://docs.confluent.io/cloud/current/client-apps/topics/manage.html)
- [Kafka Topic Configuration Reference](https://docs.confluent.io/platform/current/installation/configuration/topic-configs.html)
- [Confluent CLI Reference](https://docs.confluent.io/confluent-cli/current/command-reference/kafka/topic/index.html)
