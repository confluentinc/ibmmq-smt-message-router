#!/bin/bash

# Confluent Cloud Topic Creation Script for MQ Gateway Banking Demo
# This script creates all required Kafka topics for the demo

set -e

echo "=================================================="
echo "Creating Kafka Topics for MQ Gateway Banking Demo"
echo "=================================================="
echo ""

# Check if Confluent CLI is installed
if ! command -v confluent &> /dev/null; then
    echo "❌ Error: Confluent CLI is not installed"
    echo "Install from: https://docs.confluent.io/confluent-cli/current/install.html"
    exit 1
fi

# Check if authenticated
if ! confluent kafka cluster describe &> /dev/null; then
    echo "❌ Error: Not authenticated to Confluent Cloud"
    echo "Run: confluent login"
    echo "Then: confluent environment use <env-id>"
    echo "Then: confluent kafka cluster use <cluster-id>"
    exit 1
fi

echo "✅ Confluent CLI authenticated"
echo ""

# Topic configuration
PARTITIONS=3
REPLICATION_FACTOR=3  # Confluent Cloud default

echo "Creating topics with:"
echo "  - Partitions: $PARTITIONS"
echo "  - Replication Factor: $REPLICATION_FACTOR (Confluent Cloud managed)"
echo ""

# Function to create topic
create_topic() {
    local topic_name=$1
    local description=$2

    echo "Creating topic: $topic_name"
    echo "  Description: $description"

    if confluent kafka topic create "$topic_name" \
        --partitions $PARTITIONS \
        --config retention.ms=604800000 \
        --config cleanup.policy=delete 2>&1 | grep -q "Created topic"; then
        echo "  ✅ Created successfully"
    elif confluent kafka topic describe "$topic_name" &> /dev/null; then
        echo "  ⚠️  Topic already exists (skipping)"
    else
        echo "  ❌ Failed to create topic"
        return 1
    fi
    echo ""
}

# Create banking message topics
echo "--- Banking Message Topics ---"
create_topic "PAYMENT_DOMESTIC" "Domestic bank transfers and payments"
create_topic "PAYMENT_INTERNATIONAL" "International wire transfers and cross-border payments"
create_topic "ACCOUNT_TRANSACTION" "Account debits, credits, and balance updates"
create_topic "FRAUD_ALERT" "Fraud detection alerts and suspicious activity"

# Create operational topics
echo "--- Operational Topics ---"
create_topic "mq-unrouted-dlq" "Dead letter queue for messages without routing headers"
create_topic "mq-error-dlq" "Dead letter queue for connector processing errors"

echo ""
echo "=================================================="
echo "Topic Creation Complete!"
echo "=================================================="
echo ""

# List created topics
echo "Verifying topics:"
confluent kafka topic list | grep -E "(PAYMENT_|ACCOUNT_|FRAUD_|mq-)" || echo "No topics found with expected names"

echo ""
echo "Next steps:"
echo "  1. Deploy the IBM MQ Source Connector (see ../connector/README.md)"
echo "  2. Publish test messages (see ../../test-data/README.md)"
echo "  3. Verify routing in Confluent Cloud UI"
echo ""
