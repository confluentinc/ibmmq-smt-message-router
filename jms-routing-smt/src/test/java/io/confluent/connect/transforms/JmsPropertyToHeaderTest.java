package io.confluent.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JmsPropertyToHeaderTest {

    private JmsPropertyToHeader.Value<SinkRecord> smt;

    @BeforeEach
    void setUp() {
        smt = new JmsPropertyToHeader.Value<>();
        configure(true);
    }

    private void configure(boolean skipMissing) {
        Map<String, Object> config = new HashMap<>();
        config.put("property.name", "messageType");
        config.put("header.name", "messageType");
        config.put("skip.missing", skipMissing);
        smt.configure(config);
    }

    private static SinkRecord record(Schema schema, Object value) {
        return new SinkRecord("ibm.mq.input", 0, null, null, schema, value, 0L);
    }

    private static Map<String, Object> mapValue(String type) {
        Map<String, Object> property = new HashMap<>();
        property.put("propertyType", "string");
        property.put("string", type);
        Map<String, Object> properties = new HashMap<>();
        properties.put("messageType", property);
        Map<String, Object> value = new HashMap<>();
        value.put("text", "hello");
        value.put("properties", properties);
        return value;
    }

    private static Struct structValue(String type) {
        Schema propertySchema = SchemaBuilder.struct()
                .field("propertyType", Schema.OPTIONAL_STRING_SCHEMA)
                .field("string", Schema.OPTIONAL_STRING_SCHEMA)
                .optional().build();
        Schema propertiesSchema = SchemaBuilder.struct()
                .field("messageType", propertySchema).optional().build();
        Schema schema = SchemaBuilder.struct()
                .field("text", Schema.OPTIONAL_STRING_SCHEMA)
                .field("properties", propertiesSchema).build();
        return new Struct(schema)
                .put("text", "hello")
                .put("properties", new Struct(propertiesSchema)
                        .put("messageType", new Struct(propertySchema)
                                .put("propertyType", "string")
                                .put("string", type)));
    }

    @Test
    void copiesPropertyFromMapToHeader() {
        SinkRecord out = smt.apply(record(null, mapValue("PAYMENT")));
        Header header = out.headers().lastWithName("messageType");
        assertEquals("PAYMENT", header.value());
    }

    @Test
    void copiesPropertyFromStructToHeader() {
        Struct value = structValue("TRANSFER");
        SinkRecord out = smt.apply(record(value.schema(), value));
        assertEquals("TRANSFER", out.headers().lastWithName("messageType").value());
    }

    @Test
    void doesNotMutateInputRecordOrValue() {
        Map<String, Object> value = mapValue("PAYMENT");
        SinkRecord in = record(null, value);
        SinkRecord out = smt.apply(in);
        assertNotSame(in, out);
        assertNull(in.headers().lastWithName("messageType"));
        assertSame(value, out.value());
    }

    @Test
    void preservesExistingHeaders() {
        SinkRecord in = record(null, mapValue("PAYMENT"));
        in.headers().addString("trace", "abc");
        SinkRecord out = smt.apply(in);
        assertEquals("abc", out.headers().lastWithName("trace").value());
        assertEquals("PAYMENT", out.headers().lastWithName("messageType").value());
    }

    @Test
    void passesThroughWhenPropertyMissingAndSkipEnabled() {
        SinkRecord in = record(null, new HashMap<>(Map.of("text", "no properties")));
        assertSame(in, smt.apply(in));
    }

    @Test
    void passesThroughNullValue() {
        SinkRecord in = record(null, null);
        assertSame(in, smt.apply(in));
    }

    @Test
    void failsWhenPropertyMissingAndSkipDisabled() {
        configure(false);
        SinkRecord in = record(null, new HashMap<>(Map.of("text", "no properties")));
        assertThrows(DataException.class, () -> smt.apply(in));
    }
}
