package com.iot.devices.management.telemetry_event_processor.alerts;

import com.iot.alerts.Alert;
import com.iot.alerts.AlertRule;
import com.iot.devices.DoorSensor;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.Optional;

import static java.lang.Boolean.TRUE;

@Component
@RequiredArgsConstructor
public class DoorSensorAlertsManager implements AlertsManager<DoorSensor> {

    @Override
    public Optional<Alert> check(DoorSensor doorSensor, AlertRule alertRule) {
        return switch (alertRule.getMetricName()) {
            case BATTERY_LEVEL -> checkBattery(doorSensor.getDeviceId(), alertRule, doorSensor.getBatteryLevel());
            case TAMPER -> Objects.equals(TRUE, doorSensor.getTamperAlert()) ? createAlert(doorSensor.getDeviceId(), alertRule, null) : Optional.empty();
            case TIME_OUT -> checkTimeThreshold(doorSensor.getDeviceId(), doorSensor.getLastOpened(), alertRule);
            default -> throw new IllegalArgumentException("Unable to check " + alertRule.getMetricName() + " for " + doorSensor);
        };
    }
}
