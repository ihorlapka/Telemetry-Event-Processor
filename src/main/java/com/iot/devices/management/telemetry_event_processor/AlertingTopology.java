package com.iot.devices.management.telemetry_event_processor;

import com.iot.alerts.Alert;
import com.iot.alerts.AlertRule;
import com.iot.devices.management.telemetry_event_processor.alerts.AlertManagerProvider;
import com.iot.devices.management.telemetry_event_processor.processor.TombstoneProcessor;
import com.iot.devices.management.telemetry_event_processor.properties.KafkaStreamsProperties;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.stereotype.Component;

import java.util.*;

import static com.iot.devices.management.telemetry_event_processor.processor.TombstoneProcessor.ALERT_RULES_STORE_NAME;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.common.serialization.Serdes.ListSerde;

@Slf4j
@Component
@RequiredArgsConstructor
public class AlertingTopology {

    private final KafkaStreamsProperties properties;
    private final AlertManagerProvider alertManagerProvider;


    public KStream<String, List<Alert>> createTopology(StreamsBuilder streamsBuilder) {
        final Serde<SpecificRecord> telemetrySerde = getAvroValueSerde();
        final Serde<AlertRule> ruleValuesSerde = getAvroValueSerde();
        final Serde<List<AlertRule>> alertRulesSerde = ListSerde(ArrayList.class, ruleValuesSerde);

        final KStream<String, SpecificRecord> telemetriesStream = streamsBuilder.stream(properties.getTelemetryInputTopic(), Consumed.with(Serdes.String(), telemetrySerde));

        final KTable<String, List<AlertRule>> aggregatedRules = streamsBuilder
                .addStateStore(getStateStoreBuilder(ruleValuesSerde))
                .stream(properties.getAlertingRulesInputTopic(), Consumed.with(Serdes.String(), ruleValuesSerde))
                .processValues(TombstoneProcessor.create(), ALERT_RULES_STORE_NAME)
                .selectKey((alertRuleId, alertRule) -> alertRule.getDeviceIds())
                .flatMap(this::mapToAlertRulePerDeviceId)
                .groupByKey(Grouped.with(Serdes.String(), ruleValuesSerde))
                .aggregate(ArrayList::new,
                        (deviceId, newRule, rulesList) -> aggregateAlertRules(newRule, rulesList),
                        Materialized.with(Serdes.String(), alertRulesSerde)
                );

        final KStream<String, List<Alert>> alertsStream = telemetriesStream.join(aggregatedRules,
                alertManagerProvider::createAlert, Joined.with(Serdes.String(), telemetrySerde, alertRulesSerde));

        alertsStream
                .flatMapValues(list -> list)
                .peek((k, v) -> log.info("Alert created: {}", v))
                .to(properties.getAlertsOutputTopic());

        return alertsStream;
    }

    private StoreBuilder<KeyValueStore<String, AlertRule>> getStateStoreBuilder(Serde<AlertRule> ruleValuesSerde) {
        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(ALERT_RULES_STORE_NAME),
                Serdes.String(),
                ruleValuesSerde
        );
    }

    private List<KeyValue<String, AlertRule>> mapToAlertRulePerDeviceId(List<String> deviceIds, AlertRule alertRule) {
        return deviceIds.stream()
                .map(deviceId -> new KeyValue<>(deviceId, alertRule))
                .toList();
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

    private <T extends SpecificRecord> Serde<T> getAvroValueSerde() {
        final Serde<T> rulesSerde = new SpecificAvroSerde<>();
        rulesSerde.configure(Map.of(SCHEMA_REGISTRY_URL_CONFIG, properties.getSchemaRegistryUrl()), false);
        return rulesSerde;
    }
}
