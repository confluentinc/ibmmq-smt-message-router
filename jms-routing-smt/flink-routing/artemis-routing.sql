-- Flink SQL for routing ActiveMQ Artemis messages based on JMS properties
-- Tested with ActiveMQ Artemis 2.40.0 and Confluent Cloud for Apache Flink

-- STEP 1: Create source table for Artemis input topic
-- Drop existing table if needed
-- DROP TABLE IF EXISTS `artemis.input`;

CREATE TABLE `artemis.input` (
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
CREATE TABLE `artemis-payment` (
  messageID STRING NOT NULL,
  `timestamp` DOUBLE,
  text STRING,
  messageType STRING,
  PRIMARY KEY (messageID) NOT ENFORCED
) WITH (
  'connector' = 'confluent',
  'value.format' = 'json-registry'
);

-- Transfer topic
CREATE TABLE `artemis-transfer` (
  messageID STRING NOT NULL,
  `timestamp` DOUBLE,
  text STRING,
  messageType STRING,
  PRIMARY KEY (messageID) NOT ENFORCED
) WITH (
  'connector' = 'confluent',
  'value.format' = 'json-registry'
);

-- Notification topic
CREATE TABLE `artemis-notification` (
  messageID STRING NOT NULL,
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
INSERT INTO `artemis-payment`
SELECT
  messageID,
  `timestamp`,
  text,
  properties.messageType.`string` AS messageType
FROM `artemis.input`
WHERE properties.messageType.`string` = 'PAYMENT';

-- Route TRANSFER messages
INSERT INTO `artemis-transfer`
SELECT
  messageID,
  `timestamp`,
  text,
  properties.messageType.`string` AS messageType
FROM `artemis.input`
WHERE properties.messageType.`string` = 'TRANSFER';

-- Route NOTIFICATION messages
INSERT INTO `artemis-notification`
SELECT
  messageID,
  `timestamp`,
  text,
  properties.messageType.`string` AS messageType
FROM `artemis.input`
WHERE properties.messageType.`string` = 'NOTIFICATION';
