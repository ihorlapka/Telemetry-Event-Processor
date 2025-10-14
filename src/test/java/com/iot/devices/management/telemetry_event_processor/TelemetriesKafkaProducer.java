package com.iot.devices.management.telemetry_event_processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Slf4j
@Component
public class TelemetriesKafkaProducer {

    private final KafkaProducer<String, SpecificRecord> kafkaProducer;

    public TelemetriesKafkaProducer(TelemetriesKafkaProducerProperties producerProperties) {
        Properties properties = new Properties();
        properties.putAll(producerProperties.getProperties());
        this.kafkaProducer = new KafkaProducer<>(properties);
    }

    public void sendMessage(String topic, String key, SpecificRecord record) {
        try {
            final ProducerRecord<String, SpecificRecord> producerRecord = new ProducerRecord<>(topic, key, record);
            final Future<RecordMetadata> future = kafkaProducer.send(producerRecord);
            final RecordMetadata recordMetadata = future.get();
            if (recordMetadata.hasOffset()) {
                log.info("Message is successfully sent {}", record);
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to send message: {}", record, e);
            throw new RuntimeException("Unable to send message!");
        }

    }
}
