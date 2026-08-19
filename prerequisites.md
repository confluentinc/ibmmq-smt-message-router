# Prerequisites

## Required Infrastructure

### 1. IBM MQ Environment

You need access to an IBM MQ Queue Manager with:

- ✅ **Queue Manager** configured and running
- ✅ **Gateway Queue** created (e.g., `GATEWAY.QUEUE`)
- ✅ **Network access** from Confluent Cloud to MQ (port 1414 by default)
- ✅ **Credentials** with appropriate permissions:
  - CONNECT permission on Queue Manager
  - GET permission on Gateway Queue
  - BROWSE permission (optional, for monitoring)

**MQ Configuration Example:**
```mqsc
DEFINE QLOCAL(GATEWAY.QUEUE) MAXDEPTH(5000) REPLACE
ALTER QMGR CHLAUTH(DISABLED)  # Or configure proper channel authentication
```

**Important:** The MQ Source Connector requires that messages be in JMS format or have the `mq.message.body.jms` property set correctly to extract MQ properties as Kafka headers.

### 2. Confluent Cloud Account

- ✅ **Confluent Cloud organization** and environment
- ✅ **Kafka cluster** (Basic, Standard, or Dedicated)
- ✅ **Connect cluster** provisioned
- ✅ **API keys** for Kafka cluster and Schema Registry (if using Avro)
- ✅ **Billing enabled** (Connect clusters incur charges)

**Recommended cluster specs for demo:**
- Cluster type: Basic (cost-effective for testing)
- Cloud provider: AWS, GCP, or Azure (your choice)
- Region: Same region as MQ for lower latency

### 3. Network Connectivity

The Confluent Cloud Connect cluster must be able to reach your MQ Queue Manager:

**Option A: Public Internet (easiest for demo)**
- MQ Queue Manager has public IP or hostname
- Firewall allows inbound connections on MQ port (1414)
- Use TLS/SSL for security

**Option B: Private Networking (production)**
- AWS PrivateLink / Azure Private Link / GCP Private Service Connect
- VPN connection between Confluent Cloud and your network
- VPC Peering

**Test connectivity:**
```bash
# From a machine that can reach Confluent Cloud egress IPs
telnet your-mq-host 1414
```

If this succeeds, Confluent Cloud Connect should be able to reach MQ.

## Required Tools

### 1. Confluent CLI

Install the Confluent CLI for managing resources:

```bash
# macOS
brew install confluentinc/tap/cli

# Linux
curl -sL --http1.1 https://cnfl.io/cli | sh -s -- latest

# Windows
# Download from https://docs.confluent.io/confluent-cli/current/install.html
```

**Authenticate:**
```bash
confluent login
confluent environment list
confluent environment use <env-id>
confluent kafka cluster list
confluent kafka cluster use <cluster-id>
```

### 2. Python 3.8+

For running the test data publisher:

```bash
# Check version
python3 --version  # Should be 3.8 or higher

# Install pip if not present
python3 -m ensurepip --upgrade
```

### 3. Python Dependencies

The test publisher requires these packages (included in `requirements.txt`):

- `pymqi` - IBM MQ Python client
- `python-dotenv` - For loading environment variables

These will be installed via `pip install -r requirements.txt`.

## MQ Gateway Queue Setup

### Understanding the Gateway Pattern

The Gateway Queue pattern consolidates multiple source queues into a single queue that the connector reads from. This is achieved through:

1. **Alias Queues** (recommended)
2. **Application-level routing**
3. **MQ Forwarding Rules**

### Option 1: Alias Queues (Recommended)

Each source queue is an alias pointing to the gateway queue:

```mqsc
# Create the gateway queue
DEFINE QLOCAL(GATEWAY.QUEUE) MAXDEPTH(10000) REPLACE

# Create alias queues
DEFINE QALIAS(PAYMENT.DOMESTIC.QUEUE) TARGET(GATEWAY.QUEUE) REPLACE
DEFINE QALIAS(PAYMENT.INTERNATIONAL.QUEUE) TARGET(GATEWAY.QUEUE) REPLACE
DEFINE QALIAS(ACCOUNT.TRANSACTION.QUEUE) TARGET(GATEWAY.QUEUE) REPLACE
DEFINE QALIAS(FRAUD.ALERT.QUEUE) TARGET(GATEWAY.QUEUE) REPLACE
```

Applications publish to the alias queues, messages automatically go to `GATEWAY.QUEUE`.

### Option 2: Application-Level Routing

Applications set the `messageType` property and publish directly to `GATEWAY.QUEUE`:

```java
TextMessage msg = session.createTextMessage(payload);
msg.setStringProperty("messageType", "PAYMENT_DOMESTIC");
msg.setStringProperty("sourceQueue", "PAYMENT.DOMESTIC.QUEUE");

Queue gateway = session.createQueue("GATEWAY.QUEUE");
sender.send(gateway, msg);
```

This approach gives applications full control over routing metadata.

### Critical: MQ Message Properties

**The routing depends on MQ message properties being set correctly.**

Every message in the Gateway Queue MUST have at least:
```
messageType: [PAYMENT_DOMESTIC | PAYMENT_INTERNATIONAL | ACCOUNT_TRANSACTION | FRAUD_ALERT]
```

Optional but recommended:
```
sourceQueue: [original queue name]
priority: [HIGH | NORMAL | LOW]
businessUnit: [department/branch code]
```

These properties are automatically converted to Kafka headers by the IBM MQ Source Connector.

## Permissions & Access

### Confluent Cloud Permissions

Your user account needs:
- EnvironmentAdmin or OrganizationAdmin role, OR
- Specific permissions:
  - `ResourceOwner` on Kafka cluster
  - `ResourceOwner` on Connect cluster
  - Create topics permission

### MQ Permissions

The MQ user for the connector needs:

```mqsc
# Grant permissions to connector user
SET AUTHREC PROFILE(QM1) OBJTYPE(QMGR) PRINCIPAL('mquser') AUTHADD(CONNECT,INQ)
SET AUTHREC PROFILE(GATEWAY.QUEUE) OBJTYPE(QUEUE) PRINCIPAL('mquser') AUTHADD(GET,BROWSE,INQ)
```

## Firewall & Security

### MQ Firewall Rules

Allow inbound connections to MQ:
- **Port:** 1414 (or your custom MQ listener port)
- **Source:** Confluent Cloud egress IPs for your region
  - [Find egress IPs here](https://docs.confluent.io/cloud/current/networking/static-egress-ip-addresses.html)

### TLS/SSL (Recommended for Production)

Generate MQ certificates and configure connector with:
```json
{
  "mq.tls.truststore.location": "/path/to/truststore.jks",
  "mq.tls.truststore.password": "${secret:truststore-password}"
}
```

For this demo, you can use non-TLS connections if your MQ environment allows it.

## Cost Considerations

### Confluent Cloud Costs

- **Kafka cluster:** ~$1-2/hour (Basic tier)
- **Connect cluster:** ~$0.18/hour per connector task
- **Data transfer:** Ingress is free, egress charged per GB
- **Storage:** ~$0.10/GB/month

**Estimated demo cost:** $5-10 for a few hours of testing

**Tip:** Delete resources when done to avoid charges:
```bash
confluent connect cluster delete <connect-cluster-id>
confluent kafka cluster delete <kafka-cluster-id>
```

## Pre-Deployment Checklist

Before starting the demo, verify:

- [ ] MQ Queue Manager is accessible from the internet or via private networking
- [ ] Gateway Queue (`GATEWAY.QUEUE`) exists in MQ
- [ ] MQ user credentials are ready
- [ ] Confluent Cloud Kafka cluster is running
- [ ] Confluent Cloud Connect cluster is provisioned
- [ ] Confluent CLI is installed and authenticated
- [ ] Python 3.8+ is installed
- [ ] You have reviewed the connector configuration template
- [ ] You understand which MQ properties will be used for routing

## Optional: Test MQ Connectivity

Before deploying the connector, test MQ connectivity:

### Using Python (pymqi)

```python
import pymqi

queue_manager = 'QM1'
channel = 'SYSTEM.DEF.SVRCONN'
conn_info = 'your-mq-host(1414)'
user = 'mquser'
password = 'mqpass'

try:
    qmgr = pymqi.connect(queue_manager, channel, conn_info, user, password)
    print("✅ Successfully connected to MQ")
    qmgr.disconnect()
except pymqi.MQMIError as e:
    print(f"❌ Failed to connect: {e}")
```

### Using IBM MQ Sample Clients

```bash
# Install IBM MQ client tools
# Then test connection:
amqsputc GATEWAY.QUEUE QM1
```

## Troubleshooting Prerequisites

### Can't connect to MQ from Confluent Cloud

- Check firewall rules allow Confluent Cloud egress IPs
- Verify MQ listener is running: `netstat -an | grep 1414`
- Check MQ channel authentication: `DISPLAY CHLAUTH(*)`

### Confluent CLI authentication fails

- Ensure you have an active Confluent Cloud account
- Try `confluent logout` then `confluent login` again
- Verify your organization has billing enabled

### Python dependencies won't install

- For `pymqi`, you may need IBM MQ C client libraries:
  ```bash
  # macOS
  brew install ibm-mq
  
  # Linux - download from IBM website
  ```

If you encounter issues, see the troubleshooting section in the main README.
