-- Flink SQL for routing ActiveMQ messages based on JMS properties
-- Tested with ActiveMQ Classic 6.3.2 and Confluent Cloud for Apache Flink

-- STEP 1: Create source table for ActiveMQ input topic
-- Drop existing table if needed
-- DROP TABLE IF EXISTS `activemq.input`;

CREATE TABLE `activemq.input` (
  messageID STRING,
  `timestamp` DOUBLE,
  properties ROW<
    messageType ROW<
      propertyType STRING,
      `string` STRING
    >
  >,
  text STRING
) WITH (
  'connector' = 'confluent',
  'value.format' = 'json-registry'
);

-- STEP 2: Create sink tables for each message type
-- Payment topic
CREATE TABLE `activemq-payment` (
  messageID STRING,
  `timestamp` DOUBLE,
  text STRING,
  messageType STRING,
  PRIMARY KEY (messageID) NOT ENFORCED
) WITH (
  'connector' = 'confluent',
  'value.format' = 'json-registry'
);

-- Transfer topic
CREATE TABLE `activemq-transfer` (
  messageID STRING,
  `timestamp` DOUBLE,
  text STRING,
  messageType STRING,
  PRIMARY KEY (messageID) NOT ENFORCED
) WITH (
  'connector' = 'confluent',
  'value.format' = 'json-registry'
);

-- Notification topic
CREATE TABLE `activemq-notification` (
  messageID STRING,
  `timestamp` DOUBLE,
  text STRING,
  messageType STRING,
  PRIMARY KEY (messageID) NOT ENFORCED
) WITH (
  'connector' = 'confluent',
  'value.format' = 'json-registry'
);

-- STEP 3: Start routing jobs
-- Route PAYMENT messages
INSERT INTO `activemq-payment`
SELECT
  messageID,
  `timestamp`,
  text,
  properties.messageType.`string` AS messageType
FROM `activemq.input`
WHERE properties.messageType.`string` = 'PAYMENT';

-- Route TRANSFER messages
INSERT INTO `activemq-transfer`
SELECT
  messageID,
  `timestamp`,
  text,
  properties.messageType.`string` AS messageType
FROM `activemq.input`
WHERE properties.messageType.`string` = 'TRANSFER';

-- Route NOTIFICATION messages
INSERT INTO `activemq-notification`
SELECT
  messageID,
  `timestamp`,
  text,
  properties.messageType.`string` AS messageType
FROM `activemq.input`
WHERE properties.messageType.`string` = 'NOTIFICATION';
