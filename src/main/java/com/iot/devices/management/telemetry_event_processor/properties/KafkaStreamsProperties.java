package com.iot.devices.management.telemetry_event_processor.properties;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;

@Slf4j
@Getter
@Setter
@ToString
@Configuration
@ConfigurationProperties(KafkaStreamsProperties.PROPERTIES_PREFIX)
@RequiredArgsConstructor
public class KafkaStreamsProperties {

    final static String PROPERTIES_PREFIX = "tep.kafka";

    private Map<String, String> properties = new HashMap<>();

    @Value("${" + PROPERTIES_PREFIX + ".input-telemetries-topic}")
    private String telemetryInputTopic;

    @Value("${" + PROPERTIES_PREFIX + ".input-alerting-rules-topic}")
    private String alertingRulesInputTopic;

    @Value("${" + PROPERTIES_PREFIX + ".output-alerts-topic}")
    private String alertsOutputTopic;

    @PostConstruct
    private void logProperties() {
        log.info("kafka streams properties: {}", this);
    }

    public Properties getProperties() {
        Properties props = new Properties(properties.size());
        props.putAll(properties);
        return props;
    }

    public String getSchemaRegistryUrl() {
        return properties.get(SCHEMA_REGISTRY_URL_CONFIG);
    }
}
