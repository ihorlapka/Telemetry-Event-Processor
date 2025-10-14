package com.iot.devices.management.telemetry_event_processor;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;


@Slf4j
@Getter
@Setter
@ToString
@Configuration
@ConfigurationProperties(AlertingRulesKafkaProducerProperties.PROPERTIES_PREFIX)
@RequiredArgsConstructor
public class AlertingRulesKafkaProducerProperties {

    final static String PROPERTIES_PREFIX = "test.kafka.producer.alerting-rules";

    private Map<String, String> properties = new HashMap<>();

    @PostConstruct
    private void logProperties() {
        log.info("kafka producer alertingRules properties: {}", this);
    }
}
