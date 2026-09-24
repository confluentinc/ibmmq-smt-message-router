package io.confluent.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Flink job to route IBM MQ messages to different Kafka topics based on JMS messageType property.
 *
 * This job:
 * 1. Reads from ibm.mq.input topic
 * 2. Extracts properties.messageType.string from nested JSON
 * 3. Routes to payment-topic, transfer-topic, or notification-topic based on messageType
 *
 * Usage:
 *   flink run -c io.confluent.flink.FlinkMessageRouter target/flink-message-router-1.0.0.jar \
 *     --kafka.bootstrap.servers localhost:9092 \
 *     --kafka.api.key <api-key> \
 *     --kafka.api.secret <api-secret>
 */
public class FlinkMessageRouter {

    public static void main(String[] args) throws Exception {
        // Parse command line arguments
        String bootstrapServers = getParam(args, "--kafka.bootstrap.servers", "localhost:9092");
        String apiKey = getParam(args, "--kafka.api.key", "");
        String apiSecret = getParam(args, "--kafka.api.secret", "");

        // Set up Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // Enable checkpointing for exactly-once semantics
        env.enableCheckpointing(60000); // Checkpoint every 60 seconds

        // Build SASL configuration if API key is provided
        String saslConfig = "";
        if (!apiKey.isEmpty() && !apiSecret.isEmpty()) {
            saslConfig = String.format(
                "'properties.security.protocol' = 'SASL_SSL'," +
                "'properties.sasl.mechanism' = 'PLAIN'," +
                "'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";',",
                apiKey, apiSecret
            );
        }

        // Create source table for IBM MQ input
        String createSourceTable = String.format(
            "CREATE TABLE ibm_mq_input (" +
            "  `messageID` STRING," +
            "  `timestamp` BIGINT," +
            "  `properties` ROW<" +
            "    `messageType` ROW<" +
            "      `propertyType` STRING," +
            "      `string` STRING" +
            "    >" +
            "  >," +
            "  `text` STRING," +
            "  `proc_time` AS PROCTIME()" +
            ") WITH (" +
            "  'connector' = 'kafka'," +
            "  'topic' = 'ibm.mq.input'," +
            "  'properties.bootstrap.servers' = '%s'," +
            "  'properties.group.id' = 'flink-mq-router'," +
            "  'scan.startup.mode' = 'latest-offset'," +
            "  'format' = 'json'," +
            "  'json.fail-on-missing-field' = 'false'," +
            "  'json.ignore-parse-errors' = 'true'" +
            "  %s" +
            ")",
            bootstrapServers,
            saslConfig.isEmpty() ? "" : "," + saslConfig
        );
        tableEnv.executeSql(createSourceTable);

        // Create sink tables for each message type
        createSinkTable(tableEnv, "payment_topic", "payment-topic", bootstrapServers, saslConfig);
        createSinkTable(tableEnv, "transfer_topic", "transfer-topic", bootstrapServers, saslConfig);
        createSinkTable(tableEnv, "notification_topic", "notification-topic", bootstrapServers, saslConfig);
        createSinkTable(tableEnv, "unknown_message_type_topic", "unknown-message-type-topic", bootstrapServers, saslConfig);

        // Route PAYMENT messages
        Table paymentMessages = tableEnv.sqlQuery(
            "SELECT messageID, `timestamp`, text, properties.messageType.`string` AS messageType " +
            "FROM ibm_mq_input " +
            "WHERE properties.messageType.`string` = 'PAYMENT'"
        );
        tableEnv.createTemporaryView("payment_messages", paymentMessages);
        tableEnv.executeSql("INSERT INTO payment_topic SELECT * FROM payment_messages");

        // Route TRANSFER messages
        Table transferMessages = tableEnv.sqlQuery(
            "SELECT messageID, `timestamp`, text, properties.messageType.`string` AS messageType " +
            "FROM ibm_mq_input " +
            "WHERE properties.messageType.`string` = 'TRANSFER'"
        );
        tableEnv.createTemporaryView("transfer_messages", transferMessages);
        tableEnv.executeSql("INSERT INTO transfer_topic SELECT * FROM transfer_messages");

        // Route NOTIFICATION messages
        Table notificationMessages = tableEnv.sqlQuery(
            "SELECT messageID, `timestamp`, text, properties.messageType.`string` AS messageType " +
            "FROM ibm_mq_input " +
            "WHERE properties.messageType.`string` = 'NOTIFICATION'"
        );
        tableEnv.createTemporaryView("notification_messages", notificationMessages);
        tableEnv.executeSql("INSERT INTO notification_topic SELECT * FROM notification_messages");

        // Route unknown message types
        Table unknownMessages = tableEnv.sqlQuery(
            "SELECT messageID, `timestamp`, text, " +
            "COALESCE(properties.messageType.`string`, 'UNKNOWN') AS messageType " +
            "FROM ibm_mq_input " +
            "WHERE properties.messageType.`string` IS NULL " +
            "   OR (properties.messageType.`string` <> 'PAYMENT' " +
            "       AND properties.messageType.`string` <> 'TRANSFER' " +
            "       AND properties.messageType.`string` <> 'NOTIFICATION')"
        );
        tableEnv.createTemporaryView("unknown_messages", unknownMessages);
        tableEnv.executeSql("INSERT INTO unknown_message_type_topic SELECT * FROM unknown_messages");

        System.out.println("Flink Message Router started successfully!");
        System.out.println("Routing messages from ibm.mq.input to:");
        System.out.println("  - payment-topic");
        System.out.println("  - transfer-topic");
        System.out.println("  - notification-topic");
        System.out.println("  - unknown-message-type-topic");
    }

    private static void createSinkTable(
            StreamTableEnvironment tableEnv,
            String tableName,
            String topicName,
            String bootstrapServers,
            String saslConfig) {

        String createSinkTable = String.format(
            "CREATE TABLE %s (" +
            "  `messageID` STRING," +
            "  `timestamp` BIGINT," +
            "  `text` STRING," +
            "  `messageType` STRING," +
            "  PRIMARY KEY (`messageID`) NOT ENFORCED" +
            ") WITH (" +
            "  'connector' = 'kafka'," +
            "  'topic' = '%s'," +
            "  'properties.bootstrap.servers' = '%s'," +
            "  'format' = 'json'," +
            "  'sink.partitioner' = 'default'" +
            "  %s" +
            ")",
            tableName,
            topicName,
            bootstrapServers,
            saslConfig.isEmpty() ? "" : "," + saslConfig
        );
        tableEnv.executeSql(createSinkTable);
    }

    private static String getParam(String[] args, String name, String defaultValue) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(name)) {
                return args[i + 1];
            }
        }
        return defaultValue;
    }
}
