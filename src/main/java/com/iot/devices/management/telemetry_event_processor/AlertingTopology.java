package com.iot.devices.management.telemetry_event_processor;

import com.iot.alerts.Alert;
import com.iot.alerts.AlertRule;
import com.iot.alerts.RuleCompoundKey;
import com.iot.devices.management.telemetry_event_processor.alerts.AlertManagerProvider;
import com.iot.devices.management.telemetry_event_processor.processor.TombstoneProcessor;
import com.iot.devices.management.telemetry_event_processor.properties.KafkaStreamsProperties;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.stereotype.Component;

import java.util.*;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.common.serialization.Serdes.ListSerde;

@Slf4j
@Component
@RequiredArgsConstructor
public class AlertingTopology {

    private final KafkaStreamsProperties properties;
    private final AlertManagerProvider alertManagerProvider;


    public KStream<String, List<Alert>> createTopology(StreamsBuilder streamsBuilder) {
        final Serde<SpecificRecord> telemetrySerde = getAvroSerde(SpecificRecord.class, false);
        KStream<String, SpecificRecord> telemetriesStream = streamsBuilder.stream(properties.getTelemetryInputTopic(), Consumed.with(Serdes.String(), telemetrySerde));

        final Serde<RuleCompoundKey> ruleKeySerde = getAvroSerde(RuleCompoundKey.class, true);
        final Serde<AlertRule> ruleValuesSerde = getAvroSerde(AlertRule.class, false);
        final Serde<List<AlertRule>> alertRulesSerde = ListSerde(ArrayList.class, ruleValuesSerde);

        KTable<String, List<AlertRule>> aggregatedRules = streamsBuilder.stream(properties.getAlertingRulesInputTopic(), Consumed.with(ruleKeySerde, ruleValuesSerde))
                .processValues(TombstoneProcessor.create())
                .selectKey((k, v) -> k.getDeviceId())
                .groupByKey(Grouped.with(Serdes.String(), ruleValuesSerde))
                .aggregate(ArrayList::new,
                        (deviceId, newRule, rulesList) -> aggregateAlertRules(newRule, rulesList),
                        Materialized.with(Serdes.String(), alertRulesSerde)
                );

        KStream<String, List<Alert>> alertsStream = telemetriesStream.join(aggregatedRules,
                alertManagerProvider::createAlert, Joined.with(Serdes.String(), telemetrySerde, alertRulesSerde));

        alertsStream
                .flatMapValues(list -> list)
                .peek((k, v) -> log.info("Alert created: {}", v))
                .to(properties.getAlertsOutputTopic());

        return alertsStream;
    }

    private List<AlertRule> aggregateAlertRules(AlertRule newRule, List<AlertRule> rulesList) {
        rulesList.removeIf(presentRule -> {
            boolean isTheSameRule = presentRule.getRuleId().equals(newRule.getRuleId());
            if (isTheSameRule) {
                log.info("Removing rule from aggregated joined rules: {}", presentRule);
            }
            return isTheSameRule;
        });
        if (newRule.getIsEnabled()) {
            log.info("New rule is added to aggregated joined rules: {}", newRule);
            rulesList.add(newRule);
        }
        return rulesList;
    }

    private <T extends SpecificRecord> Serde<T> getAvroSerde(Class<T> clazz, boolean isKey) {
        final Serde<T> rulesSerde = new SpecificAvroSerde<>();
        rulesSerde.configure(Map.of(SCHEMA_REGISTRY_URL_CONFIG, properties.getSchemaRegistryUrl()), isKey);
        return rulesSerde;
    }
}
