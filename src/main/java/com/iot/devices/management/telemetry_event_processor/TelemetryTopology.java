package com.iot.devices.management.telemetry_event_processor;

import com.iot.devices.management.telemetry_event_processor.properties.KafkaStreamsProperties;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Map;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

@Slf4j
@RequiredArgsConstructor
public class TelemetryTopology {

    private final StreamsBuilder streamsBuilder;
    private final KafkaStreamsProperties properties;

    public void createTopology() {
        final Serde<SpecificRecord> valueSerde = new SpecificAvroSerde<>();
        valueSerde.configure(Map.of(SCHEMA_REGISTRY_URL_CONFIG, properties.getSchemaRegistryUrl()), false);
        KStream<String, SpecificRecord> stream = streamsBuilder.stream(properties.getTelemetryInputTopic(), Consumed.with(Serdes.String(), valueSerde));
        stream.peek((k, v) -> log.info("key: {}, value: {}", k, v))
                .to(properties.getAlertsOutputTopic());
    }
}
