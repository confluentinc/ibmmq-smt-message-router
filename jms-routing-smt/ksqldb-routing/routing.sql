-- ksqlDB Message Routing for IBM MQ Messages
--
-- This approach routes messages from the ibm.mq.input topic to separate topics
-- based on the nested JMS property: properties.messageType.string
--
-- Prerequisites:
-- 1. IBM MQ Source Connector writing to 'ibm.mq.input' topic
-- 2. ksqlDB cluster running in Confluent Cloud or on-premises
--
-- Usage:
-- 1. Connect to your ksqlDB cluster
-- 2. Run these statements in order

-- ============================================================================
-- STEP 1: Create the source stream from IBM MQ input topic
-- ============================================================================

CREATE STREAM ibm_mq_input_stream (
  messageID VARCHAR,
  messageType VARCHAR,
  timestamp BIGINT,
  deliveryMode INT,
  correlationID VARCHAR,
  replyTo VARCHAR,
  destination STRUCT<
    destinationType VARCHAR,
    name VARCHAR
  >,
  redelivered BOOLEAN,
  type VARCHAR,
  expiration BIGINT,
  priority INT,
  properties STRUCT<
    JMS_IBM_Format STRUCT<
      propertyType VARCHAR,
      string VARCHAR
    >,
    JMS_IBM_PutDate STRUCT<
      propertyType VARCHAR,
      string VARCHAR
    >,
    JMS_IBM_Character_Set STRUCT<
      propertyType VARCHAR,
      string VARCHAR
    >,
    JMSXDeliveryCount STRUCT<
      propertyType VARCHAR,
      integer INT
    >,
    messageType STRUCT<
      propertyType VARCHAR,
      string VARCHAR
    >,
    JMS_IBM_MsgType STRUCT<
      propertyType VARCHAR,
      integer INT
    >,
    JMSXUserID STRUCT<
      propertyType VARCHAR,
      string VARCHAR
    >,
    JMS_IBM_Encoding STRUCT<
      propertyType VARCHAR,
      integer INT
    >,
    JMS_IBM_PutTime STRUCT<
      propertyType VARCHAR,
      string VARCHAR
    >,
    JMSXAppID STRUCT<
      propertyType VARCHAR,
      string VARCHAR
    >,
    JMS_IBM_PutApplType STRUCT<
      propertyType VARCHAR,
      integer INT
    >
  >,
  bytes VARCHAR,
  map VARCHAR,
  text VARCHAR
) WITH (
  KAFKA_TOPIC='ibm.mq.input',
  VALUE_FORMAT='JSON'
);

-- ============================================================================
-- STEP 2: Create routed streams for each message type
-- ============================================================================

-- Route PAYMENT messages
CREATE STREAM payment_topic WITH (
  KAFKA_TOPIC='payment-topic',
  VALUE_FORMAT='JSON',
  PARTITIONS=3,
  REPLICAS=3
) AS
SELECT
  messageID,
  timestamp,
  text,
  properties->messageType->string AS messageType
FROM ibm_mq_input_stream
WHERE properties->messageType->string = 'PAYMENT'
EMIT CHANGES;

-- Route TRANSFER messages
CREATE STREAM transfer_topic WITH (
  KAFKA_TOPIC='transfer-topic',
  VALUE_FORMAT='JSON',
  PARTITIONS=3,
  REPLICAS=3
) AS
SELECT
  messageID,
  timestamp,
  text,
  properties->messageType->string AS messageType
FROM ibm_mq_input_stream
WHERE properties->messageType->string = 'TRANSFER'
EMIT CHANGES;

-- Route NOTIFICATION messages
CREATE STREAM notification_topic WITH (
  KAFKA_TOPIC='notification-topic',
  VALUE_FORMAT='JSON',
  PARTITIONS=3,
  REPLICAS=3
) AS
SELECT
  messageID,
  timestamp,
  text,
  properties->messageType->string AS messageType
FROM ibm_mq_input_stream
WHERE properties->messageType->string = 'NOTIFICATION'
EMIT CHANGES;

-- ============================================================================
-- STEP 3: Optional - Create a catch-all stream for unknown message types
-- ============================================================================

CREATE STREAM unknown_message_type_topic WITH (
  KAFKA_TOPIC='unknown-message-type-topic',
  VALUE_FORMAT='JSON',
  PARTITIONS=3,
  REPLICAS=3
) AS
SELECT
  messageID,
  timestamp,
  text,
  properties->messageType->string AS messageType
FROM ibm_mq_input_stream
WHERE properties->messageType->string IS NULL
   OR (properties->messageType->string != 'PAYMENT'
       AND properties->messageType->string != 'TRANSFER'
       AND properties->messageType->string != 'NOTIFICATION')
EMIT CHANGES;

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Show all streams
SHOW STREAMS;

-- Describe the payment stream
DESCRIBE payment_topic;

-- Query payment messages in real-time
SELECT * FROM payment_topic EMIT CHANGES;

-- Count messages by type
SELECT
  properties->messageType->string AS messageType,
  COUNT(*) AS message_count
FROM ibm_mq_input_stream
GROUP BY properties->messageType->string
EMIT CHANGES;
