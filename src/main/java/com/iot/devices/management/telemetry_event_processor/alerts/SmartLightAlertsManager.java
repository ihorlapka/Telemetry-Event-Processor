package com.iot.devices.management.telemetry_event_processor.alerts;

import com.iot.alerts.Alert;
import com.iot.alerts.AlertRule;
import com.iot.devices.SmartLight;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Optional;

import static com.iot.alerts.MetricType.ENERGY_CONSUMED;
import static java.util.Optional.empty;

@Component
@RequiredArgsConstructor
public class SmartLightAlertsManager implements AlertsManager<SmartLight> {

    @Override
    public Optional<Alert> check(SmartLight smartLight, AlertRule alertRule) {
        if (ENERGY_CONSUMED.equals(alertRule.getMetricName())) {
            return checkThreshold(alertRule, smartLight.getPowerConsumption());
        }
        return empty();
    }
}
