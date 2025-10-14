package com.iot.devices.management.telemetry_event_processor.config;

import com.iot.alerts.Alert;
import com.iot.devices.management.telemetry_event_processor.TelemetryTopology;
import com.iot.devices.management.telemetry_event_processor.properties.KafkaStreamsProperties;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.streams.KafkaStreamsMicrometerListener;

import java.util.List;

import static org.springframework.kafka.config.StreamsBuilderFactoryBean.Listener;

@Slf4j
@Configuration
public class StreamConfig {

    @Bean
    public Listener kafkaStreamsMicrometerListener(MeterRegistry meterRegistry) {
        return new KafkaStreamsMicrometerListener(meterRegistry);
    }

    @Primary
    @Bean
    public StreamsBuilderFactoryBean defaultStreamsBuilder(Listener kafkaStreamsMicrometerListener,
                                                           KafkaStreamsProperties kafkaStreamsProperties) {
        StreamsBuilderFactoryBean streamsBuilderFactory = new StreamsBuilderFactoryBean();
        streamsBuilderFactory.setAutoStartup(true);
        streamsBuilderFactory.setStreamsConfiguration(kafkaStreamsProperties.getProperties());
        streamsBuilderFactory.addListener(kafkaStreamsMicrometerListener);
        return streamsBuilderFactory;
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_BUILDER_BEAN_NAME + "Topology")
    public KStream<String, List<Alert>> topologyBuilder(StreamsBuilder streamsBuilder, TelemetryTopology telemetryTopology) {
        return telemetryTopology.createTopology(streamsBuilder);
    }
}
