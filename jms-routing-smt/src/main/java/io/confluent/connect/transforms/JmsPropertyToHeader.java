package io.confluent.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;

/**
 * Extracts a nested JMS property from a JMS source connector message and copies it to a Kafka header.
 *
 * <p>JMS source connectors store JMS properties in a nested structure like:
 * <pre>
 * {
 *   "properties": {
 *     "messageType": {
 *       "propertyType": "string",
 *       "string": "PAYMENT"
 *     }
 *   }
 * }
 * </pre>
 *
 * <p>This SMT reads {@code properties.<property.name>.string} and adds it as a Kafka header.
 * The record value is not modified, and neither is the incoming record: a new record with a copy
 * of the headers is returned.
 *
 * <p>Only the {@code $Value} variant is provided. For most use cases the built-in
 * {@code ExtractTopic$Value} with {@code field.format=JSON_PATH} is simpler; see the README.
 */
public abstract class JmsPropertyToHeader<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Extract a nested JMS property from the message value and copy it to a Kafka header";

    private static final String PROPERTY_NAME_CONFIG = "property.name";
    private static final String HEADER_NAME_CONFIG = "header.name";
    private static final String SKIP_MISSING_CONFIG = "skip.missing";

    private static final String PROPERTIES_FIELD = "properties";
    private static final String STRING_FIELD = "string";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(PROPERTY_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    "Name of the JMS property to extract (e.g., 'messageType')")
            .define(HEADER_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    "Name of the Kafka header to create")
            .define(SKIP_MISSING_CONFIG, ConfigDef.Type.BOOLEAN, true,
                    ConfigDef.Importance.MEDIUM,
                    "If true, pass records through unchanged when the property is missing or null. "
                            + "If false, fail with a DataException.");

    private String propertyName;
    private String headerName;
    private boolean skipMissing;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        propertyName = config.getString(PROPERTY_NAME_CONFIG);
        headerName = config.getString(HEADER_NAME_CONFIG);
        skipMissing = config.getBoolean(SKIP_MISSING_CONFIG);
    }

    @Override
    public R apply(R record) {
        final Object value = record.value();
        if (value == null) {
            return record;
        }

        final String propertyValue = extract(value);
        if (propertyValue == null) {
            if (skipMissing) {
                return record;
            }
            throw new DataException("Property '" + propertyName
                    + "' not found in message value of type " + value.getClass().getName()
                    + " and skip.missing=false");
        }

        final ConnectHeaders headers = new ConnectHeaders();
        record.headers().forEach(headers::add);
        headers.addString(headerName, propertyValue);

        return record.newRecord(record.topic(), record.kafkaPartition(),
                record.keySchema(), record.key(),
                record.valueSchema(), record.value(),
                record.timestamp(), headers);
    }

    /**
     * Walks {@code properties -> <propertyName> -> string}. Each level may be a {@link Struct}
     * (schema-based) or a {@link Map} (schemaless); a missing level yields {@code null}.
     */
    private String extract(Object value) {
        final Object properties = child(value, PROPERTIES_FIELD);
        final Object property = child(properties, propertyName);
        final Object string = child(property, STRING_FIELD);
        return string != null ? string.toString() : null;
    }

    private static Object child(Object parent, String field) {
        if (parent instanceof Struct) {
            final Struct struct = (Struct) parent;
            return struct.schema().field(field) != null ? struct.get(field) : null;
        }
        if (parent instanceof Map) {
            return ((Map<?, ?>) parent).get(field);
        }
        return null;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    public static class Value<R extends ConnectRecord<R>> extends JmsPropertyToHeader<R> {
    }
}
