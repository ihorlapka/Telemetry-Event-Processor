package com.iot.devices.management.telemetry_event_processor.alerts;

import com.iot.alerts.Alert;
import com.iot.alerts.AlertRule;
import com.iot.devices.TemperatureSensor;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@RequiredArgsConstructor
public class TemperatureSensorAlertManager implements AlertsManager<TemperatureSensor> {

    @Override
    public Optional<Alert> check(TemperatureSensor temperatureSensor, AlertRule alertRule) {
        return switch (alertRule.getMetricName()) {
            case TEMPERATURE -> checkThreshold(temperatureSensor.getDeviceId(), alertRule, temperatureSensor.getTemperature());
            case HUMIDITY -> checkThreshold(temperatureSensor.getDeviceId(), alertRule, temperatureSensor.getHumidity());
            case PRESSURE -> checkThreshold(temperatureSensor.getDeviceId(), alertRule, temperatureSensor.getPressure());
            default -> throw new IllegalArgumentException("Unable to check " + alertRule.getMetricName() + " for " + temperatureSensor);
        };
    }
}
