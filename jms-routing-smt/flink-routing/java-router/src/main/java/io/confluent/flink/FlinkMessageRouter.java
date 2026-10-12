package io.confluent.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Self-managed Flink job that routes JMS messages to different Kafka topics based on the
 * JMS {@code messageType} property.
 *
 * <p>This job:
 * <ol>
 *   <li>Reads plain JSON from the {@code ibm.mq.input} topic (connector {@code output.data.format=JSON})</li>
 *   <li>Extracts {@code properties.messageType.string} from the nested JSON</li>
 *   <li>Routes to payment-topic, transfer-topic or notification-topic; anything else (including
 *       a missing property) goes to unknown-message-type-topic</li>
 * </ol>
 *
 * <p>All four routes are submitted as a single job (one read of the input topic). For Confluent
 * Cloud for Apache Flink use the SQL files instead; they use the managed {@code confluent}
 * connector and Schema Registry.
 *
 * <pre>
 * flink run -c io.confluent.flink.FlinkMessageRouter target/flink-message-router-1.0.0.jar \
 *   --kafka.bootstrap.servers localhost:9092 \
 *   --kafka.api.key &lt;api-key&gt; \
 *   --kafka.api.secret &lt;api-secret&gt;
 * </pre>
 */
public class FlinkMessageRouter {

    private static final String INPUT_TOPIC = "ibm.mq.input";
    private static final String CONSUMER_GROUP = "flink-mq-router";

    private static final String[][] ROUTES = {
        // table name, topic, messageType value
        {"payment_topic", "payment-topic", "PAYMENT"},
        {"transfer_topic", "transfer-topic", "TRANSFER"},
        {"notification_topic", "notification-topic", "NOTIFICATION"},
    };
    private static final String UNKNOWN_TABLE = "unknown_message_type_topic";
    private static final String UNKNOWN_TOPIC = "unknown-message-type-topic";

    public static void main(String[] args) throws Exception {
        String bootstrapServers = getParam(args, "--kafka.bootstrap.servers", "localhost:9092");
        String apiKey = getParam(args, "--kafka.api.key", "");
        String apiSecret = getParam(args, "--kafka.api.secret", "");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Checkpointing is required for the Kafka sink to commit; EXACTLY_ONCE delivery additionally
        // needs 'sink.delivery-guarantee' = 'exactly-once' and a transactional id prefix.
        env.enableCheckpointing(60000);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
                env, EnvironmentSettings.newInstance().inStreamingMode().build());

        String security = securityOptions(apiKey, apiSecret);

        tableEnv.executeSql(
            "CREATE TABLE ibm_mq_input (" +
            "  `messageID` STRING," +
            "  `timestamp` DOUBLE," +
            "  `properties` ROW<`messageType` ROW<`propertyType` STRING, `string` STRING>>," +
            "  `text` STRING" +
            ") WITH (" +
            "  'connector' = 'kafka'," +
            "  'topic' = '" + INPUT_TOPIC + "'," +
            "  'properties.bootstrap.servers' = '" + escape(bootstrapServers) + "'," +
            "  'properties.group.id' = '" + CONSUMER_GROUP + "'," +
            "  'scan.startup.mode' = 'latest-offset'," +
            "  'format' = 'json'," +
            "  'json.fail-on-missing-field' = 'false'," +
            "  'json.ignore-parse-errors' = 'true'" +
            security +
            ")");

        StatementSet routes = tableEnv.createStatementSet();

        for (String[] route : ROUTES) {
            createSinkTable(tableEnv, route[0], route[1], bootstrapServers, security);
            routes.addInsertSql(
                "INSERT INTO " + route[0] + " " +
                "SELECT messageID, `timestamp`, text, properties.messageType.`string` AS messageType " +
                "FROM ibm_mq_input " +
                "WHERE properties.messageType.`string` = '" + route[2] + "'");
        }

        createSinkTable(tableEnv, UNKNOWN_TABLE, UNKNOWN_TOPIC, bootstrapServers, security);
        routes.addInsertSql(
            "INSERT INTO " + UNKNOWN_TABLE + " " +
            "SELECT messageID, `timestamp`, text, " +
            "  COALESCE(properties.messageType.`string`, 'UNKNOWN') AS messageType " +
            "FROM ibm_mq_input " +
            "WHERE properties.messageType.`string` IS NULL " +
            "   OR properties.messageType.`string` NOT IN ('PAYMENT', 'TRANSFER', 'NOTIFICATION')");

        routes.execute();

        System.out.println("Flink Message Router submitted. Routing " + INPUT_TOPIC + " to:");
        for (String[] route : ROUTES) {
            System.out.println("  - " + route[1]);
        }
        System.out.println("  - " + UNKNOWN_TOPIC);
    }

    private static void createSinkTable(
            StreamTableEnvironment tableEnv,
            String tableName,
            String topicName,
            String bootstrapServers,
            String security) {

        // The plain 'kafka' connector is append-only and does not accept a PRIMARY KEY.
        tableEnv.executeSql(
            "CREATE TABLE " + tableName + " (" +
            "  `messageID` STRING," +
            "  `timestamp` DOUBLE," +
            "  `text` STRING," +
            "  `messageType` STRING" +
            ") WITH (" +
            "  'connector' = 'kafka'," +
            "  'topic' = '" + topicName + "'," +
            "  'properties.bootstrap.servers' = '" + escape(bootstrapServers) + "'," +
            "  'format' = 'json'" +
            security +
            ")");
    }

    /** Returns extra WITH options (each prefixed by a comma), or "" when no credentials are set. */
    private static String securityOptions(String apiKey, String apiSecret) {
        if (apiKey.isEmpty() || apiSecret.isEmpty()) {
            return "";
        }
        List<String> options = new ArrayList<>();
        options.add("'properties.security.protocol' = 'SASL_SSL'");
        options.add("'properties.sasl.mechanism' = 'PLAIN'");
        String jaas = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                + apiKey + "\" password=\"" + apiSecret + "\";";
        options.add("'properties.sasl.jaas.config' = '" + escape(jaas) + "'");
        return "," + String.join(",", options);
    }

    /** Escapes a value for use inside a single-quoted Flink SQL string literal. */
    private static String escape(String value) {
        return value.replace("'", "''");
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
