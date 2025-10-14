package com.iot.devices.management.telemetry_event_processor.alerts;

import com.iot.alerts.Alert;
import com.iot.alerts.AlertRule;
import com.iot.devices.Thermostat;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@RequiredArgsConstructor
public class ThermostatAlertsManager implements AlertsManager<Thermostat> {

    @Override
    public Optional<Alert> check(Thermostat thermostat, AlertRule alertRule) {
        return switch (alertRule.getMetricName()) {
            case TEMPERATURE -> checkThreshold(alertRule, thermostat.getCurrentTemperature());
            case HUMIDITY -> checkThreshold(alertRule, thermostat.getHumidity());
            default -> throw new IllegalArgumentException("Unable to check " + alertRule.getMetricName() + " for " + thermostat);
        };
    }
}
