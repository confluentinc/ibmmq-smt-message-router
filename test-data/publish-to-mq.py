#!/usr/bin/env python3
"""
IBM MQ Test Message Publisher for Banking Demo

Publishes sample banking messages to IBM MQ Gateway Queue with proper
routing metadata (messageType header) for SMT-based routing in Confluent Cloud.
"""

import json
import argparse
import sys
from pathlib import Path
from datetime import datetime
import time

try:
    import pymqi
except ImportError:
    print("❌ Error: pymqi module not installed")
    print("Install with: pip install pymqi")
    print("Note: pymqi requires IBM MQ client libraries")
    sys.exit(1)

# Message type to file mapping
MESSAGE_TYPES = {
    "PAYMENT_DOMESTIC": "sample-messages/payment-domestic.json",
    "PAYMENT_INTERNATIONAL": "sample-messages/payment-international.json",
    "ACCOUNT_TRANSACTION": "sample-messages/account-transaction.json",
    "FRAUD_ALERT": "sample-messages/fraud-alert.json"
}

# Routing metadata for each message type
MESSAGE_METADATA = {
    "PAYMENT_DOMESTIC": {
        "priority": "NORMAL",
        "businessUnit": "RETAIL_BANKING",
        "sourceQueue": "PAYMENT.DOMESTIC.QUEUE"
    },
    "PAYMENT_INTERNATIONAL": {
        "priority": "HIGH",
        "businessUnit": "CORPORATE_BANKING",
        "sourceQueue": "PAYMENT.INTERNATIONAL.QUEUE"
    },
    "ACCOUNT_TRANSACTION": {
        "priority": "NORMAL",
        "businessUnit": "RETAIL_BANKING",
        "sourceQueue": "ACCOUNT.TRANSACTION.QUEUE"
    },
    "FRAUD_ALERT": {
        "priority": "CRITICAL",
        "businessUnit": "SECURITY",
        "sourceQueue": "FRAUD.ALERT.QUEUE"
    }
}


def load_config(config_file='mq-config.properties'):
    """Load MQ configuration from properties file."""
    config = {}
    config_path = Path(config_file)

    if not config_path.exists():
        print(f"❌ Error: Config file not found: {config_file}")
        print("Create it from the template: cp mq-config.properties.example mq-config.properties")
        sys.exit(1)

    with open(config_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                if '=' in line:
                    key, value = line.split('=', 1)
                    config[key.strip()] = value.strip()

    # Validate required fields
    required = ['mq.hostname', 'mq.port', 'mq.queue.manager', 'mq.channel',
                'mq.queue.name', 'mq.username', 'mq.password']
    missing = [field for field in required if field not in config]
    if missing:
        print(f"❌ Error: Missing required config fields: {', '.join(missing)}")
        sys.exit(1)

    return config


def load_message_payload(message_type):
    """Load message payload from JSON file."""
    file_path = Path(MESSAGE_TYPES[message_type])

    if not file_path.exists():
        print(f"❌ Error: Sample message file not found: {file_path}")
        sys.exit(1)

    with open(file_path) as f:
        return json.load(f)


def connect_to_mq(config):
    """Connect to IBM MQ."""
    try:
        conn_info = f"{config['mq.hostname']}({config['mq.port']})"

        print(f"Connecting to MQ...")
        print(f"  Host: {config['mq.hostname']}")
        print(f"  Port: {config['mq.port']}")
        print(f"  Queue Manager: {config['mq.queue.manager']}")
        print(f"  Channel: {config['mq.channel']}")

        qmgr = pymqi.connect(
            config['mq.queue.manager'],
            config['mq.channel'],
            conn_info,
            config.get('mq.username'),
            config.get('mq.password')
        )

        print("✅ Connected to MQ successfully\n")
        return qmgr

    except pymqi.MQMIError as e:
        print(f"❌ Failed to connect to MQ:")
        print(f"  Reason Code: {e.reason}")
        print(f"  Error: {e}")
        sys.exit(1)


def publish_message(queue, message_type, payload, metadata, correlation_id):
    """
    Publish a message to MQ with routing properties.

    MQ properties (messageType, priority, etc.) are automatically converted
    to Kafka headers by the IBM MQ Source Connector.
    """
    try:
        # Create message descriptor
        md = pymqi.MD()
        md.Format = pymqi.CMQC.MQFMT_STRING
        md.CorrelId = correlation_id.encode('utf-8')

        # Set message properties for routing (these become Kafka headers)
        pmo = pymqi.PMO()
        pmo.Options = pymqi.CMQC.MQPMO_NO_SYNCPOINT

        # Create property handle for message properties
        cmho = pymqi.CMHO()
        message_handle = pymqi.MessageHandle(cmho)

        # Set routing properties
        pd_messageType = pymqi.PD()
        pd_messageType.Options = pymqi.CMQC.MQPD_NONE
        message_handle.set_property('messageType', pymqi.CMQC.MQTYPE_STRING, message_type, pd=pd_messageType)

        pd_priority = pymqi.PD()
        pd_priority.Options = pymqi.CMQC.MQPD_NONE
        message_handle.set_property('priority', pymqi.CMQC.MQTYPE_STRING, metadata['priority'], pd=pd_priority)

        pd_sourceQueue = pymqi.PD()
        pd_sourceQueue.Options = pymqi.CMQC.MQPD_NONE
        message_handle.set_property('sourceQueue', pymqi.CMQC.MQTYPE_STRING, metadata['sourceQueue'], pd=pd_sourceQueue)

        pd_businessUnit = pymqi.PD()
        pd_businessUnit.Options = pymqi.CMQC.MQPD_NONE
        message_handle.set_property('businessUnit', pymqi.CMQC.MQTYPE_STRING, metadata['businessUnit'], pd=pd_businessUnit)

        # Set message handle in PMO
        pmo.OriginalMsgHandle = message_handle.msg_handle

        # Convert payload to JSON string
        payload_json = json.dumps(payload, indent=2)

        # Put message to queue
        queue.put(payload_json.encode('utf-8'), md, pmo)

        return True

    except pymqi.MQMIError as e:
        print(f"❌ Failed to publish message:")
        print(f"  Reason Code: {e.reason}")
        print(f"  Error: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Publish test banking messages to IBM MQ Gateway Queue',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Publish one of each message type
  python publish-to-mq.py

  # Publish 10 messages of each type
  python publish-to-mq.py --count 10

  # Publish only payment messages
  python publish-to-mq.py --type PAYMENT_DOMESTIC --count 5

  # Use custom config file
  python publish-to-mq.py --config my-mq-config.properties
        '''
    )
    parser.add_argument(
        '--config',
        default='mq-config.properties',
        help='MQ configuration file (default: mq-config.properties)'
    )
    parser.add_argument(
        '--type',
        choices=list(MESSAGE_TYPES.keys()),
        help='Publish only specific message type (default: all types)'
    )
    parser.add_argument(
        '--count',
        type=int,
        default=1,
        help='Number of messages to publish per type (default: 1)'
    )
    parser.add_argument(
        '--delay',
        type=float,
        default=0.1,
        help='Delay in seconds between messages (default: 0.1)'
    )

    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)

    # Connect to MQ
    qmgr = connect_to_mq(config)

    try:
        # Open queue
        queue = pymqi.Queue(qmgr, config['mq.queue.name'])
        print(f"✅ Opened queue: {config['mq.queue.name']}\n")

        # Determine which message types to publish
        types_to_publish = [args.type] if args.type else list(MESSAGE_TYPES.keys())

        total_published = 0
        total_failed = 0

        print("=" * 60)
        print("Publishing Messages")
        print("=" * 60)

        for message_type in types_to_publish:
            print(f"\n📤 Publishing {args.count} x {message_type}")

            # Load base payload
            base_payload = load_message_payload(message_type)
            metadata = MESSAGE_METADATA[message_type]

            for i in range(args.count):
                # Create unique payload for each message
                payload = base_payload.copy()

                # Update transaction ID to be unique
                if 'transactionId' in payload:
                    payload['transactionId'] = f"{payload['transactionId']}-{i+1:03d}"
                elif 'alertId' in payload:
                    payload['alertId'] = f"{payload['alertId']}-{i+1:03d}"

                # Update timestamp to current time
                payload['timestamp'] = datetime.utcnow().isoformat() + 'Z'

                # Generate correlation ID
                correlation_id = f"DEMO-{message_type}-{int(time.time())}-{i+1}"

                # Publish message
                success = publish_message(queue, message_type, payload, metadata, correlation_id)

                if success:
                    total_published += 1
                    print(f"  ✅ [{i+1}/{args.count}] Published with correlation ID: {correlation_id}")
                else:
                    total_failed += 1
                    print(f"  ❌ [{i+1}/{args.count}] Failed to publish")

                # Delay between messages
                if i < args.count - 1:
                    time.sleep(args.delay)

        print("\n" + "=" * 60)
        print("Summary")
        print("=" * 60)
        print(f"✅ Successfully published: {total_published}")
        if total_failed > 0:
            print(f"❌ Failed: {total_failed}")
        print(f"\nMessages are in queue: {config['mq.queue.name']}")
        print("The IBM MQ Source Connector will:")
        print("  1. Read messages from the Gateway Queue")
        print("  2. Extract MQ properties as Kafka headers")
        print("  3. Route to topics based on messageType header")
        print("\nNext steps:")
        print("  - Verify connector is running")
        print("  - Check Confluent Cloud UI → Topics → Messages")
        print("  - Messages should appear in corresponding topics:")
        for msg_type in types_to_publish:
            print(f"    • {msg_type}")

        queue.close()

    except pymqi.MQMIError as e:
        print(f"❌ MQ Error:")
        print(f"  Reason Code: {e.reason}")
        print(f"  Error: {e}")
        sys.exit(1)
    finally:
        qmgr.disconnect()
        print("\n✅ Disconnected from MQ")


if __name__ == '__main__':
    main()
