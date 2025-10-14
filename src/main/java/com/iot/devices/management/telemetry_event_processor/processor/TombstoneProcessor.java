package com.iot.devices.management.telemetry_event_processor.processor;

import com.iot.alerts.AlertRule;
import com.iot.alerts.RuleCompoundKey;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

@Slf4j
@RequiredArgsConstructor(staticName = "create")
public class TombstoneProcessor implements FixedKeyProcessorSupplier<RuleCompoundKey, AlertRule, AlertRule> {

    @Override
    public FixedKeyProcessor<RuleCompoundKey, AlertRule, AlertRule> get() {
        return new FixedKeyProcessor<>() {

            private FixedKeyProcessorContext<RuleCompoundKey, AlertRule> context;

            @Override
            public void init(FixedKeyProcessorContext<RuleCompoundKey, AlertRule> context) {
                this.context = context;
            }

            @Override
            public void process(FixedKeyRecord<RuleCompoundKey, AlertRule> record) {
                if (record.value() == null) {
                    log.info("Creating a tombstone marker for removing alert rule");
                    final AlertRule tombstoneMarker = AlertRule.newBuilder()
                            .setRuleId(record.key().getRuleId())
                            .setDeviceId(record.key().getDeviceId())
                            .setIsEnabled(false)
                            .build();

                    context.forward(record.withValue(tombstoneMarker));
                }
                context.forward(record);
            }
        };
    }
}
