package io.confluent.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Extracts nested JMS properties from IBM MQ connector messages and copies them to Kafka headers.
 *
 * The IBM MQ connector stores JMS properties in a nested structure like:
 * {
 *   "properties": {
 *     "messageType": {
 *       "propertyType": "string",
 *       "string": "PAYMENT"
 *     }
 *   }
 * }
 *
 * This SMT extracts the value from properties.<propertyName>.string and copies it to a Kafka header.
 */
public abstract class JmsPropertyToHeader<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC = "Extract JMS properties from IBM MQ message value and copy to Kafka headers";

    private static final String PROPERTY_NAME_CONFIG = "property.name";
    private static final String HEADER_NAME_CONFIG = "header.name";
    private static final String SKIP_MISSING_CONFIG = "skip.missing";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(PROPERTY_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    "Name of the JMS property to extract (e.g., 'messageType')")
            .define(HEADER_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    "Name of the Kafka header to create")
            .define(SKIP_MISSING_CONFIG, ConfigDef.Type.BOOLEAN, true,
                    ConfigDef.Importance.MEDIUM,
                    "Skip records where the property is missing or null");

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
        if (record.value() == null) {
            return record;
        }

        Object value = record.value();
        String propertyValue = null;

        // Handle Schema-based (Struct) values
        if (value instanceof Struct) {
            propertyValue = extractFromStruct((Struct) value);
        }
        // Handle schema-less (Map) values
        else if (value instanceof Map) {
            propertyValue = extractFromMap((Map<String, Object>) value);
        }

        // If property not found or null, skip or fail based on config
        if (propertyValue == null) {
            if (skipMissing) {
                return record;
            } else {
                // Build detailed error message
                String errorDetails = buildErrorDetails(value);
                throw new IllegalArgumentException(
                    "Property '" + propertyName + "' not found in message and skip.missing=false. " +
                    "Message structure: " + errorDetails);
            }
        }

        // Add the property value as a Kafka header
        Headers headers = record.headers();
        headers.addString(headerName, propertyValue);

        return record;
    }

    /**
     * Extract property value from schema-based Struct
     * Path: properties -> <propertyName> -> string
     */
    private String extractFromStruct(Struct struct) {
        try {
            // Try to get properties as Struct first
            Object propertiesObj = null;
            try {
                propertiesObj = struct.get("properties");
            } catch (Exception e) {
                // Field might not exist
                return null;
            }

            if (propertiesObj == null) {
                return null;
            }

            // Handle if properties is a Struct
            if (propertiesObj instanceof Struct) {
                Struct properties = (Struct) propertiesObj;
                Object propertyObj = null;
                try {
                    propertyObj = properties.get(propertyName);
                } catch (Exception e) {
                    return null;
                }

                if (propertyObj == null) {
                    return null;
                }

                // Handle if property is a Struct
                if (propertyObj instanceof Struct) {
                    Struct property = (Struct) propertyObj;
                    try {
                        Object stringValue = property.get("string");
                        return stringValue != null ? stringValue.toString() : null;
                    } catch (Exception e) {
                        return null;
                    }
                }
                // Handle if property is a Map (mixed schema/schemaless)
                else if (propertyObj instanceof Map) {
                    Map<String, Object> property = (Map<String, Object>) propertyObj;
                    Object stringValue = property.get("string");
                    return stringValue != null ? stringValue.toString() : null;
                }
            }
            // Handle if properties is a Map (mixed schema/schemaless)
            else if (propertiesObj instanceof Map) {
                Map<String, Object> properties = (Map<String, Object>) propertiesObj;
                Object propertyObj = properties.get(propertyName);
                if (propertyObj == null) {
                    return null;
                }

                if (propertyObj instanceof Map) {
                    Map<String, Object> property = (Map<String, Object>) propertyObj;
                    Object stringValue = property.get("string");
                    return stringValue != null ? stringValue.toString() : null;
                }
                else if (propertyObj instanceof Struct) {
                    Struct property = (Struct) propertyObj;
                    try {
                        Object stringValue = property.get("string");
                        return stringValue != null ? stringValue.toString() : null;
                    } catch (Exception e) {
                        return null;
                    }
                }
            }

            return null;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Extract property value from schema-less Map
     * Path: properties -> <propertyName> -> string
     */
    private String extractFromMap(Map<String, Object> map) {
        try {
            Object propertiesObj = map.get("properties");
            if (!(propertiesObj instanceof Map)) {
                return null;
            }

            Map<String, Object> properties = (Map<String, Object>) propertiesObj;
            Object propertyObj = properties.get(propertyName);
            if (!(propertyObj instanceof Map)) {
                return null;
            }

            Map<String, Object> property = (Map<String, Object>) propertyObj;
            Object stringValue = property.get("string");

            return stringValue != null ? stringValue.toString() : null;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Build detailed error message showing available fields
     */
    private String buildErrorDetails(Object value) {
        StringBuilder details = new StringBuilder();

        if (value instanceof Struct) {
            Struct struct = (Struct) value;
            details.append("Struct with schema fields: ");
            details.append(struct.schema().fields().stream()
                .map(field -> field.name())
                .collect(Collectors.joining(", ")));
        } else if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            details.append("Map with keys: ");
            details.append(String.join(", ", map.keySet().stream()
                .map(Object::toString)
                .collect(Collectors.toList())));
        } else {
            details.append("Unknown type: ").append(value.getClass().getName());
        }

        return details.toString();
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    public static class Value<R extends ConnectRecord<R>> extends JmsPropertyToHeader<R> {
        @Override
        public R apply(R record) {
            return super.apply(record);
        }
    }
}
