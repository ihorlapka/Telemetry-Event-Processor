package com.iot.devices.management.telemetry_event_processor;

import com.iot.alerts.Alert;
import com.iot.alerts.AlertRule;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;

@Disabled
@Slf4j
class SchemaCompatibilityTest {

    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    @Test
    void compatibilityTest() throws IOException, RestClientException {
        compareSchemas(Alert.getClassSchema());
        compareSchemas(AlertRule.getClassSchema());
    }

    private void compareSchemas(Schema schema) throws IOException, RestClientException {
        try (CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(SCHEMA_REGISTRY_URL, 10)) {
            boolean isCompatible = client.testCompatibility(schema.getFullName(), new AvroSchema(schema));
            if (isCompatible) {
                log.info("{} schema is compatible with the Registry!", schema.getName());
            } else {
                log.info("{} Schema is NOT compatible with Registry!", schema.getName());
            }
        }
    }
}
