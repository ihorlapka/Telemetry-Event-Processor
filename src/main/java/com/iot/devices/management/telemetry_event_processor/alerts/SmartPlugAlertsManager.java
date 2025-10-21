package com.iot.devices.management.telemetry_event_processor.alerts;

import com.iot.alerts.Alert;
import com.iot.alerts.AlertRule;
import com.iot.devices.SmartPlug;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@RequiredArgsConstructor
public class SmartPlugAlertsManager implements AlertsManager<SmartPlug> {

    @Override
    public Optional<Alert> check(SmartPlug smartPlug, AlertRule alertRule) {
        return switch (alertRule.getMetricName()) {
            case VOLTAGE -> checkThreshold(smartPlug.getDeviceId(), alertRule, smartPlug.getVoltage());
            case CURRENT -> checkThreshold(smartPlug.getDeviceId(), alertRule, smartPlug.getCurrent());
            case POWER -> checkThreshold(smartPlug.getDeviceId(), alertRule, smartPlug.getPowerUsage());
            default -> throw new IllegalArgumentException("Unable to check " + alertRule.getMetricName() + " for " + smartPlug);
        };
    }
}
