package com.iot.devices.management.telemetry_event_processor.processor;

import com.iot.alerts.AlertRule;
import com.iot.alerts.RuleCompoundKey;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;

@RequiredArgsConstructor(staticName = "create")
public class PreRemoveProcessor implements FixedKeyProcessorSupplier<RuleCompoundKey, AlertRule, AlertRule> {

    private ProcessorContext processorContext;

    @Override
    public FixedKeyProcessor<RuleCompoundKey, AlertRule, AlertRule> get() {
        return record -> {
            if (record.value() == null) {
                final AlertRule tombstoneMarker = AlertRule.newBuilder()
                        .setRuleId(record.key().getRuleId())
                        .setDeviceId(record.key().getDeviceId())
                        .setIsEnabled(false)
                        .build();
                processorContext.forward(record.key(), tombstoneMarker);
            }
            processorContext.forward(record.key(), record.value());
        };
    }
}
