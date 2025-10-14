package com.iot.devices.management.telemetry_event_processor;

import com.iot.alerts.AlertRule;
import com.iot.alerts.RuleCompoundKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Slf4j
@Component
public class AlertingRulesKafkaProducer {

    private final KafkaProducer<RuleCompoundKey, AlertRule> kafkaProducer;

    public AlertingRulesKafkaProducer(AlertingRulesKafkaProducerProperties producerProperties) {
        Properties properties = new Properties();
        properties.putAll(producerProperties.getProperties());
        this.kafkaProducer = new KafkaProducer<>(properties);
    }

    public void sendMessage(String topic, RuleCompoundKey key, AlertRule record) {
        try {
            final ProducerRecord<RuleCompoundKey, AlertRule> producerRecord = new ProducerRecord<>(topic, key, record);
            final Future<RecordMetadata> future = kafkaProducer.send(producerRecord);
            final RecordMetadata recordMetadata = future.get();
            if (recordMetadata.hasOffset()) {
                log.info("AlertRule is successfully sent {}", record);
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to send Alert Rule: {}", record, e);
            throw new RuntimeException("Unable to send Alert Rule!");
        }

    }
}
