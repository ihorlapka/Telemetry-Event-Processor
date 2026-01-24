package com.iot.devices.management.telemetry_event_processor.processor;

import com.iot.alerts.AlertRule;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;


@Slf4j
@RequiredArgsConstructor(staticName = "create")
public class TombstoneProcessor implements FixedKeyProcessorSupplier<String, AlertRule, AlertRule> {

    public static final String ALERT_RULES_STORE_NAME = "alert-rules-state-store";

    @Override
    public FixedKeyProcessor<String, AlertRule, AlertRule> get() {
        return new FixedKeyProcessor<>() {

            private FixedKeyProcessorContext<String, AlertRule> context;
            private KeyValueStore<String, AlertRule> ruleStore;

            @Override
            public void init(FixedKeyProcessorContext<String, AlertRule> context) {
                this.context = context;
                this.ruleStore = context.getStateStore(ALERT_RULES_STORE_NAME);
            }

            @Override
            public void process(FixedKeyRecord<String, AlertRule> record) {
                final String ruleId = record.key();
                final AlertRule currentRule = record.value();
                if (currentRule == null) {
                    final AlertRule lastAlertRule = ruleStore.get(ruleId);
                    if (lastAlertRule != null) {
                        log.info("Creating a tombstone marker for removing alert ruleId={}", ruleId);
                        final AlertRule tombstoneMarker = AlertRule.newBuilder()
                                .setDeviceIds(lastAlertRule.getDeviceIds())
                                .setRuleId(ruleId)
                                .setIsEnabled(false)
                                .build();

                        ruleStore.delete(ruleId);
                        context.forward(record.withValue(tombstoneMarker));
                    } else {
                        log.warn("No previous alert rule was found in state store with ruleId={}, there is nothing to delete", ruleId);
                    }
                } else {
                    ruleStore.put(ruleId, currentRule);
                    context.forward(record);
                }
            }
        };
    }
}