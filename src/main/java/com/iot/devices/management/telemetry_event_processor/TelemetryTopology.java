package com.iot.devices.management.telemetry_event_processor;

import com.iot.alerts.Alert;
import com.iot.alerts.AlertRule;
import com.iot.alerts.RuleCompoundKey;
import com.iot.devices.management.telemetry_event_processor.alerts.AlertManagerProvider;
import com.iot.devices.management.telemetry_event_processor.processor.PreRemoveProcessor;
import com.iot.devices.management.telemetry_event_processor.properties.KafkaStreamsProperties;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.common.serialization.Serdes.ListSerde;

@Slf4j
@RequiredArgsConstructor
public class TelemetryTopology {

    private final StreamsBuilder streamsBuilder;
    private final KafkaStreamsProperties properties;

    public void createTopology() {
        final Serde<SpecificRecord> telemetrySerde = getAvroSerde(SpecificRecord.class, false);
        KStream<String, SpecificRecord> telemetriesStream = streamsBuilder.stream(properties.getTelemetryInputTopic(), Consumed.with(Serdes.String(), telemetrySerde));

        final Serde<RuleCompoundKey> ruleKeySerde = getAvroSerde(RuleCompoundKey.class, true);
        final Serde<AlertRule> ruleValuesSerde = getAvroSerde(AlertRule.class, false);
        final Serde<List<AlertRule>> alertRulesSerde = ListSerde(ArrayList.class, ruleValuesSerde);

        KTable<String, List<AlertRule>> aggregatedRules = streamsBuilder.stream(properties.getAlertingRulesInputTopic(), Consumed.with(ruleKeySerde, ruleValuesSerde))
                .processValues(PreRemoveProcessor.create())
                .selectKey((k, v) -> k.getDeviceId())
                .groupByKey(Grouped.with(Serdes.String(), ruleValuesSerde))
                .aggregate(ArrayList::new,
                        (deviceId, newRule, rulesList) -> {
                            rulesList.removeIf(r -> r.getRuleId().equals(newRule.getRuleId()));
                            if (newRule.getIsEnabled()) {
                                rulesList.add(newRule);
                            }
                            return rulesList;
                        },
                        Materialized.with(Serdes.String(), alertRulesSerde)
                );

        final ValueJoiner<SpecificRecord, List<AlertRule>, List<Alert>> joiner = (telemetry, alertRules) ->
                new AlertManagerProvider().createAlert(telemetry, alertRules);

        telemetriesStream.join(aggregatedRules, joiner, Joined.with(Serdes.String(), telemetrySerde, alertRulesSerde))
                .peek((k, v) -> log.info("Alert created {}", v))
                .to(properties.getAlertsOutputTopic());
    }

    private <T extends SpecificRecord> Serde<T> getAvroSerde(Class<T> clazz, boolean isKey) {
        final Serde<T> rulesSerde = new SpecificAvroSerde<>();
        rulesSerde.configure(Map.of(SCHEMA_REGISTRY_URL_CONFIG, properties.getSchemaRegistryUrl()), isKey);
        return rulesSerde;
    }
}
