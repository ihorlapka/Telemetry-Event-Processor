package com.iot.devices.management.telemetry_event_processor;

import com.iot.alerts.*;
import com.iot.devices.DeviceStatus;
import com.iot.devices.EnergyMeter;
import com.iot.devices.management.telemetry_event_processor.properties.KafkaStreamsProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.apache.commons.io.ThreadUtils;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static com.iot.alerts.SeverityLevel.CRITICAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;


@Slf4j
@Testcontainers
@ActiveProfiles("test")
@SpringBootTest
class TelemetryEventProcessorApplicationTests {

    @Container
    static KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.9.0"));

    @DynamicPropertySource
    static void kafkaProps(DynamicPropertyRegistry registry) {
        registry.add("test.kafka.producer.telemetries.properties.bootstrap.servers", kafkaContainer::getBootstrapServers);
        registry.add("test.kafka.producer.alerting-rules.properties.bootstrap.servers", kafkaContainer::getBootstrapServers);
        registry.add("test.kafka.consumer.properties.bootstrap.servers", kafkaContainer::getBootstrapServers);
        registry.add("tep.kafka.properties.bootstrap.servers", kafkaContainer::getBootstrapServers);
    }

    @Autowired
    KafkaStreamsProperties streamsProperties;
    @Autowired
    KafkaConsumerProperties consumerProperties;
    @Autowired
    TelemetriesKafkaProducer telemetriesKafkaProducer;
    @Autowired
    AlertingRulesKafkaProducer alertingRulesKafkaProducer;

    @BeforeAll
    public static void setUp() {
        kafkaContainer.start();
        try (AdminClient adminClient = AdminClient.create(Map.of("bootstrap.servers", kafkaContainer.getBootstrapServers()))) {
            adminClient.createTopics(Collections.singletonList(new NewTopic("iot-devices-data", 1, (short) 1)));
            adminClient.createTopics(Collections.singletonList(new NewTopic("iot-alerting-rules", 1, (short) 1)
                    .configs(Map.of("cleanup.policy", "compact"))));
            adminClient.createTopics(Collections.singletonList(new NewTopic("iot-devices-alerts", 1, (short) 1)));
        }
    }

    @Test
    public void testKafkaStreams() throws InterruptedException {
        String deviceId = UUID.randomUUID().toString();

        AlertRule alertRule = AlertRule.newBuilder()
                .setRuleId(UUID.randomUUID().toString())
                .setDeviceId(deviceId)
                .setMetricName(MetricType.VOLTAGE)
                .setThresholdType(ThresholdType.GREATER_THAN)
                .setThresholdValue(225f)
                .setSeverity(CRITICAL)
                .setIsEnabled(true)
                .build();

        RuleCompoundKey key = RuleCompoundKey.newBuilder()
                .setDeviceId(deviceId)
                .setRuleId(alertRule.getRuleId())
                .build();

        alertingRulesKafkaProducer.sendMessage(streamsProperties.getAlertingRulesInputTopic(), key, alertRule);

        ThreadUtils.sleep(Duration.ofSeconds(1));

        EnergyMeter energyMeter1 = EnergyMeter.newBuilder()
                .setDeviceId(deviceId)
                .setVoltage(220f)
                .setStatus(DeviceStatus.ONLINE)
                .setFirmwareVersion("1.01v")
                .setLastUpdated(Instant.now())
                .build();

        telemetriesKafkaProducer.sendMessage(streamsProperties.getTelemetryInputTopic(), energyMeter1.getDeviceId(), energyMeter1);

        EnergyMeter energyMeter2 = EnergyMeter.newBuilder()
                .setDeviceId(deviceId)
                .setVoltage(223f)
                .setStatus(DeviceStatus.ONLINE)
                .setFirmwareVersion("1.01v")
                .setLastUpdated(Instant.now())
                .build();

        telemetriesKafkaProducer.sendMessage(streamsProperties.getTelemetryInputTopic(), energyMeter2.getDeviceId(), energyMeter2);

        EnergyMeter energyMeter3 = EnergyMeter.newBuilder()
                .setDeviceId(deviceId)
                .setVoltage(226f)
                .setCurrent(0.1f)
                .setPower(110f)
                .setEnergyConsumed(1000f)
                .setStatus(DeviceStatus.ONLINE)
                .setFirmwareVersion("1.01v")
                .setLastUpdated(Instant.now())
                .build();

        telemetriesKafkaProducer.sendMessage(streamsProperties.getTelemetryInputTopic(), energyMeter3.getDeviceId(), energyMeter3);

        final Properties properties = getProperties();
        try (KafkaConsumer<String, Alert> kafkaConsumer = new KafkaConsumer<>(properties)) {
            kafkaConsumer.subscribe(Collections.singletonList(streamsProperties.getAlertsOutputTopic()));
            ConsumerRecords<String, Alert> records = kafkaConsumer.poll(Duration.of(500, ChronoUnit.MILLIS));

            assertFalse(records.isEmpty());
            assertEquals(1, records.count());

            records.forEach(record -> {
                Alert receivedAlert = record.value();
                log.info("Received: {}", receivedAlert);
                assertEquals(deviceId, receivedAlert.getDeviceId());
                assertEquals(alertRule.getRuleId(), receivedAlert.getRuleId());
                assertEquals(CRITICAL, receivedAlert.getSeverity());
                assertEquals(energyMeter3.getVoltage(), receivedAlert.getActualValue());
            });
        }
    }

    private Properties getProperties() {
        final Properties properties = new Properties(consumerProperties.getProperties().size());
        properties.putAll(consumerProperties.getProperties());
        return properties;
    }

}
