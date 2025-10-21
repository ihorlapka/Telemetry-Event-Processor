package com.iot.devices.management.telemetry_event_processor.alerts;

import com.iot.alerts.Alert;
import com.iot.alerts.AlertRule;
import com.iot.devices.SoilMoistureSensor;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@RequiredArgsConstructor
public class SoilMoistureSensorAlertsManager implements AlertsManager<SoilMoistureSensor> {

    @Override
    public Optional<Alert> check(SoilMoistureSensor soilMoistureSensor, AlertRule alertRule) {
        return switch (alertRule.getMetricName()) {
            case PERCENTAGE -> checkThreshold(soilMoistureSensor.getDeviceId(), alertRule, soilMoistureSensor.getMoisturePercentage());
            case BATTERY_LEVEL -> checkBattery(soilMoistureSensor.getDeviceId(), alertRule, soilMoistureSensor.getBatteryLevel());
            case TEMPERATURE -> checkThreshold(soilMoistureSensor.getDeviceId(), alertRule, soilMoistureSensor.getSoilTemperature());
            default -> throw new IllegalArgumentException("Unable to check " + alertRule.getMetricName() + " for " + soilMoistureSensor);
        };
    }
}
