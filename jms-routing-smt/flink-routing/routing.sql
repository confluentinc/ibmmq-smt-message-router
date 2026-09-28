-- Apache Flink SQL Message Routing for IBM MQ Messages
--
-- This approach uses Flink SQL to route messages from the ibm.mq.input topic
-- to separate topics based on the nested JMS property: properties.messageType.string
--
-- Prerequisites:
-- 1. IBM MQ Source Connector writing to 'ibm.mq.input' topic
-- 2. Flink compute pool in Confluent Cloud
-- 3. JSON schema registered in Schema Registry for ibm.mq.input topic
--
-- Usage in Confluent Cloud for Apache Flink:
-- 1. Navigate to Flink in Confluent Cloud
-- 2. Select your compute pool
-- 3. Open SQL workspace
-- 4. Run these statements in order (each one separately)
--
-- IMPORTANT: Schema Registry Setup Required
-- Before running these statements, ensure a JSON schema is registered for the
-- ibm.mq.input topic in Schema Registry. The schema must NOT contain null types
-- (Flink doesn't support org.everit.json.schema.NullSchema).
--
-- If you get schema errors:
-- 1. Delete the existing schema from Schema Registry
-- 2. Let Flink create the schema when you create the source table
-- 3. Or register a cleaned schema without null types

-- ============================================================================
-- STEP 1: Create source table for IBM MQ input
-- ============================================================================
-- Maps to existing 'ibm.mq.input' Kafka topic
-- Uses JSON Schema Registry for serialization

CREATE TABLE `ibm.mq.input` (
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

-- ============================================================================
-- STEP 2: Create sink tables for each message type
-- ============================================================================
-- These will write to payment-topic, transfer-topic, notification-topic
-- Schemas will be auto-created in Schema Registry

-- Payment messages sink
CREATE TABLE `payment-topic` (
  messageID STRING,
  `timestamp` DOUBLE,
  text STRING,
  messageType STRING,
  PRIMARY KEY (messageID) NOT ENFORCED
) WITH (
  'connector' = 'confluent',
  'value.format' = 'json-registry'
);

-- Transfer messages sink
CREATE TABLE `transfer-topic` (
  messageID STRING,
  `timestamp` DOUBLE,
  text STRING,
  messageType STRING,
  PRIMARY KEY (messageID) NOT ENFORCED
) WITH (
  'connector' = 'confluent',
  'value.format' = 'json-registry'
);

-- Notification messages sink
CREATE TABLE `notification-topic` (
  messageID STRING,
  `timestamp` DOUBLE,
  text STRING,
  messageType STRING,
  PRIMARY KEY (messageID) NOT ENFORCED
) WITH (
  'connector' = 'confluent',
  'value.format' = 'json-registry'
);

-- ============================================================================
-- STEP 3: Start routing jobs (continuous INSERT statements)
-- ============================================================================
-- Each INSERT runs as a continuous streaming job with:
-- - Exactly-once processing semantics
-- - Event-time processing with watermarks
-- - Automatic checkpointing
-- - Sub-5 second latency in steady state
--
-- Run each INSERT statement separately - they will run continuously

-- Route PAYMENT messages
INSERT INTO `payment-topic`
SELECT
  messageID,
  `timestamp`,
  text,
  properties.messageType.`string` AS messageType
FROM `ibm.mq.input`
WHERE properties.messageType.`string` = 'PAYMENT';

-- Route TRANSFER messages
INSERT INTO `transfer-topic`
SELECT
  messageID,
  `timestamp`,
  text,
  properties.messageType.`string` AS messageType
FROM `ibm.mq.input`
WHERE properties.messageType.`string` = 'TRANSFER';

-- Route NOTIFICATION messages
INSERT INTO `notification-topic`
SELECT
  messageID,
  `timestamp`,
  text,
  properties.messageType.`string` AS messageType
FROM `ibm.mq.input`
WHERE properties.messageType.`string` = 'NOTIFICATION';

-- ============================================================================
-- VERIFICATION
-- ============================================================================
-- After starting the jobs, check:
-- 1. Jobs tab - should show 3 running jobs
-- 2. Send test messages to IBM MQ
-- 3. Check output topics (payment-topic, transfer-topic, notification-topic)
-- 4. Expect ~1-5 second end-to-end latency

-- Query to see incoming messages (run in a separate session):
-- SELECT * FROM `ibm.mq.input` LIMIT 10;

-- Query to see routed payment messages:
-- SELECT * FROM `payment-topic` LIMIT 10;

-- Count messages by type:
-- SELECT
--   properties.messageType.`string` AS messageType,
--   COUNT(*) AS message_count
-- FROM `ibm.mq.input`
-- GROUP BY properties.messageType.`string`;

-- ============================================================================
-- TROUBLESHOOTING
-- ============================================================================
--
-- Error: "Topic 'ibm_mq_input' collides with existing topic: ibm.mq.input"
-- Solution: Use backticks around table names with dots: `ibm.mq.input`
--
-- Error: "Unsupported format: json"
-- Solution: Use 'value.format' = 'json-registry' (not 'json')
--
-- Error: "Schema doesn't match"
-- Solution: Delete schema from Schema Registry and let Flink create it fresh
--
-- Error: "Unsupported JSON schema type org.everit.json.schema.NullSchema"
-- Solution: Remove all "type": "null" fields from the JSON schema
--
-- Error: "Table already exists"
-- Solution: DROP TABLE `table-name` first
--
-- Jobs running but no output:
-- - Check job metrics in Jobs tab (records in/out)
-- - Verify watermark settings (default is 12 minutes on event time)
-- - After initial checkpoint, latency should be sub-5 seconds
-- - Look for exceptions in job details
