-- Apache Flink SQL Message Routing for IBM MQ Messages
--
-- This approach uses Flink SQL to route messages from ibm.mq.input topic
-- to separate topics based on the nested JMS property: properties.messageType.string
--
-- Can be executed in:
-- - Confluent Cloud for Apache Flink
-- - Self-managed Flink cluster with Kafka connector
-- - Flink SQL Client
--
-- Prerequisites:
-- 1. IBM MQ Source Connector writing to 'ibm.mq.input' topic
-- 2. Flink cluster with Kafka connector (flink-connector-kafka)
-- 3. Kafka cluster accessible from Flink

-- ============================================================================
-- Configuration (Confluent Cloud for Apache Flink)
-- ============================================================================

-- Set these in Confluent Cloud UI or via properties:
-- kafka.bootstrap.servers = <your-kafka-cluster>
-- security.protocol = SASL_SSL
-- sasl.mechanism = PLAIN
-- sasl.jaas.config = org.apache.kafka.common.security.plain.PlainLoginModule required username="<api-key>" password="<api-secret>";

-- ============================================================================
-- STEP 1: Create source table for IBM MQ input
-- ============================================================================

CREATE TABLE ibm_mq_input (
  `messageID` STRING,
  `messageType` STRING,
  `timestamp` BIGINT,
  `deliveryMode` INT,
  `correlationID` STRING,
  `replyTo` STRING,
  `destination` ROW<
    `destinationType` STRING,
    `name` STRING
  >,
  `redelivered` BOOLEAN,
  `type` STRING,
  `expiration` BIGINT,
  `priority` INT,
  `properties` ROW<
    `JMS_IBM_Format` ROW<
      `propertyType` STRING,
      `string` STRING
    >,
    `JMS_IBM_PutDate` ROW<
      `propertyType` STRING,
      `string` STRING
    >,
    `JMS_IBM_Character_Set` ROW<
      `propertyType` STRING,
      `string` STRING
    >,
    `JMSXDeliveryCount` ROW<
      `propertyType` STRING,
      `integer` INT
    >,
    `messageType` ROW<
      `propertyType` STRING,
      `string` STRING
    >,
    `JMS_IBM_MsgType` ROW<
      `propertyType` STRING,
      `integer` INT
    >,
    `JMSXUserID` ROW<
      `propertyType` STRING,
      `string` STRING
    >,
    `JMS_IBM_Encoding` ROW<
      `propertyType` STRING,
      `integer` INT
    >,
    `JMS_IBM_PutTime` ROW<
      `propertyType` STRING,
      `string` STRING
    >,
    `JMSXAppID` ROW<
      `propertyType` STRING,
      `string` STRING
    >,
    `JMS_IBM_PutApplType` ROW<
      `propertyType` STRING,
      `integer` INT
    >
  >,
  `bytes` STRING,
  `map` STRING,
  `text` STRING,
  `proc_time` AS PROCTIME()  -- Processing time for windowing
) WITH (
  'connector' = 'kafka',
  'topic' = 'ibm.mq.input',
  'properties.bootstrap.servers' = '${kafka.bootstrap.servers}',
  'properties.group.id' = 'flink-mq-router',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'true'
);

-- ============================================================================
-- STEP 2: Create sink tables for each message type
-- ============================================================================

-- Payment messages sink
CREATE TABLE payment_topic (
  `messageID` STRING,
  `timestamp` BIGINT,
  `text` STRING,
  `messageType` STRING,
  PRIMARY KEY (`messageID`) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'payment-topic',
  'properties.bootstrap.servers' = '${kafka.bootstrap.servers}',
  'format' = 'json',
  'sink.partitioner' = 'default'
);

-- Transfer messages sink
CREATE TABLE transfer_topic (
  `messageID` STRING,
  `timestamp` BIGINT,
  `text` STRING,
  `messageType` STRING,
  PRIMARY KEY (`messageID`) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'transfer-topic',
  'properties.bootstrap.servers' = '${kafka.bootstrap.servers}',
  'format' = 'json',
  'sink.partitioner' = 'default'
);

-- Notification messages sink
CREATE TABLE notification_topic (
  `messageID` STRING,
  `timestamp` BIGINT,
  `text` STRING,
  `messageType` STRING,
  PRIMARY KEY (`messageID`) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'notification-topic',
  'properties.bootstrap.servers' = '${kafka.bootstrap.servers}',
  'format' = 'json',
  'sink.partitioner' = 'default'
);

-- Unknown message type sink
CREATE TABLE unknown_message_type_topic (
  `messageID` STRING,
  `timestamp` BIGINT,
  `text` STRING,
  `messageType` STRING,
  PRIMARY KEY (`messageID`) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'unknown-message-type-topic',
  'properties.bootstrap.servers' = '${kafka.bootstrap.servers}',
  'format' = 'json',
  'sink.partitioner' = 'default'
);

-- ============================================================================
-- STEP 3: Insert data into sink tables (routing logic)
-- ============================================================================

-- Route PAYMENT messages
INSERT INTO payment_topic
SELECT
  messageID,
  `timestamp`,
  text,
  properties.messageType.`string` AS messageType
FROM ibm_mq_input
WHERE properties.messageType.`string` = 'PAYMENT';

-- Route TRANSFER messages
INSERT INTO transfer_topic
SELECT
  messageID,
  `timestamp`,
  text,
  properties.messageType.`string` AS messageType
FROM ibm_mq_input
WHERE properties.messageType.`string` = 'TRANSFER';

-- Route NOTIFICATION messages
INSERT INTO notification_topic
SELECT
  messageID,
  `timestamp`,
  text,
  properties.messageType.`string` AS messageType
FROM ibm_mq_input
WHERE properties.messageType.`string` = 'NOTIFICATION';

-- Route unknown/null message types
INSERT INTO unknown_message_type_topic
SELECT
  messageID,
  `timestamp`,
  text,
  COALESCE(properties.messageType.`string`, 'UNKNOWN') AS messageType
FROM ibm_mq_input
WHERE properties.messageType.`string` IS NULL
   OR (properties.messageType.`string` <> 'PAYMENT'
       AND properties.messageType.`string` <> 'TRANSFER'
       AND properties.messageType.`string` <> 'NOTIFICATION');

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Count messages by type (continuous query)
SELECT
  properties.messageType.`string` AS messageType,
  COUNT(*) AS message_count
FROM ibm_mq_input
GROUP BY properties.messageType.`string`;

-- View recent payment messages
SELECT * FROM payment_topic LIMIT 10;

-- Monitor throughput per message type
SELECT
  TUMBLE_START(proc_time, INTERVAL '1' MINUTE) AS window_start,
  properties.messageType.`string` AS messageType,
  COUNT(*) AS message_count
FROM ibm_mq_input
GROUP BY
  TUMBLE(proc_time, INTERVAL '1' MINUTE),
  properties.messageType.`string`;
