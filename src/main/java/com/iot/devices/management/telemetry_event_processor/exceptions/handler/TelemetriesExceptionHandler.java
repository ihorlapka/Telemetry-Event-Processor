package com.iot.devices.management.telemetry_event_processor.exceptions.handler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ErrorHandlerContext;

import java.util.Map;

@Slf4j
public class TelemetriesExceptionHandler implements DeserializationExceptionHandler {

    @Override
    public void configure(Map<String, ?> configs) {
        //no implementation needed
    }

    @Override
    public DeserializationHandlerResponse handle(ErrorHandlerContext context, ConsumerRecord<byte[], byte[]> record, Exception exception) {
        log.error("Deserialization exception, Topic: {}, Offset: {}, Partition: {}",
                record.topic(), record.offset(), record.partition(), exception);
        return DeserializationHandlerResponse.CONTINUE;
    }
}
