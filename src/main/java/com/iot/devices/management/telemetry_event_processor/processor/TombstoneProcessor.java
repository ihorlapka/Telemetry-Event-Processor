package com.iot.devices.management.telemetry_event_processor.processor;

import com.iot.alerts.AlertRule;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;


@Slf4j
@RequiredArgsConstructor(staticName = "create")
public class TombstoneProcessor implements FixedKeyProcessorSupplier<String, AlertRule, AlertRule> {

    @Override
    public FixedKeyProcessor<String, AlertRule, AlertRule> get() {
        return new FixedKeyProcessor<>() {

            private FixedKeyProcessorContext<String, AlertRule> context;

            @Override
            public void init(FixedKeyProcessorContext<String, AlertRule> context) {
                this.context = context;
            }

            @Override
            public void process(FixedKeyRecord<String, AlertRule> record) {
                if (record.value() == null) {
                    log.info("Creating a tombstone marker for removing alert rule");
                    final AlertRule tombstoneMarker = AlertRule.newBuilder()
                            .setRuleId(record.key())
                            .setIsEnabled(false)
                            .build();

                    context.forward(record.withValue(tombstoneMarker));
                }
                context.forward(record);
            }
        };
    }
}
