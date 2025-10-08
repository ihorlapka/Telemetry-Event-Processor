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


@Slf4j
@Getter
@Setter
@ToString
@Configuration
@ConfigurationProperties(KafkaStreamsProperties.PROPERTIES_PREFIX)
@RequiredArgsConstructor
public class KafkaStreamsProperties {

    final static String PROPERTIES_PREFIX = "tep.kafka";

    @Value("${" + PROPERTIES_PREFIX + ".input-telemetries-topic}")
    private String telemetryInputTopic;

    @Value("${" + PROPERTIES_PREFIX + ".output-alerts-topic}")
    private String alertsOutputTopic;

    @Value("${spring.kafka.streams.schema.registry.url}")
    private String schemaRegistryUrl;

    @PostConstruct
    private void logProperties() {
        log.info("kafka streams properties: {}", this);
    }

}
