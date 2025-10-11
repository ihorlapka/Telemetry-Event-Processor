package com.iot.devices.management.telemetry_event_processor.alerts;

import com.iot.alerts.Alert;
import com.iot.alerts.AlertRule;
import com.iot.devices.DoorSensor;
import lombok.RequiredArgsConstructor;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.util.Optional;

import static com.iot.alerts.ThresholdType.GREATER_THAN;
import static com.iot.alerts.ThresholdType.LESS_THAN;
import static java.util.Optional.empty;

@Component
@RequiredArgsConstructor
public class DoorSensorAlertsManager implements AlertsManager<DoorSensor> {

    @Override
    public Optional<Alert> checkEnergyMeter(DoorSensor doorSensor, AlertRule alertRule) {
        return switch (alertRule.getMetricName()) {
            case BATTERY_LEVEL -> checkBattery(alertRule, doorSensor.getBatteryLevel());
            case TAMPER -> createAlert(alertRule, null);
//            case OPENED -> checkTimeThreshold(doorSensor.getLastOpened(), );
            default -> throw new IllegalArgumentException("Unable to check " + alertRule.getMetricName() + " for " + doorSensor);
        };
    }

    private Optional<Alert> checkBattery(AlertRule alertRule, @Nullable Integer batteryLevel) {
        if (batteryLevel != null && checkThreshold(LESS_THAN, batteryLevel, alertRule)) {
            return createAlert(alertRule, (float) batteryLevel);
        }
        return empty();
    }

//    private Optional<Alert> check(AlertRule alertRule, DoorSensor doorSensor) {
//        if (doorSensor.getLastOpened() )
//    }
}
