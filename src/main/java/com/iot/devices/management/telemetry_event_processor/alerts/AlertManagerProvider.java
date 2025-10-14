package com.iot.devices.management.telemetry_event_processor.alerts;

import com.iot.alerts.Alert;
import com.iot.alerts.AlertRule;
import com.iot.devices.*;
import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Component
@RequiredArgsConstructor
public class AlertManagerProvider {

    private final DoorSensorAlertsManager doorSensorAlertsManager;
    private final EnergyMeterAlertsManager energyMeterAlertsManager;
    private final SmartLightAlertsManager smartLightAlertsManager;
    private final SmartPlugAlertsManager smartPlugAlertsManager;
    private final SoilMoistureSensorAlertsManager soilMoistureSensorAlertsManager;
    private final TemperatureSensorAlertManager temperatureSensorAlertManager;
    private final ThermostatAlertsManager thermostatAlertsManager;

    public List<Alert> createAlert(SpecificRecord telemetry, List<AlertRule> alertRules) {
        final List<Alert> alerts = new ArrayList<>();
        for (AlertRule alertRule : alertRules) {
            verify(telemetry, alertRule).ifPresent(alerts::add);
        }
        return alerts;
    }

    private Optional<Alert> verify(SpecificRecord telemetry, AlertRule alertRule) {
        return switch (telemetry) {
            case DoorSensor ds -> doorSensorAlertsManager.check(ds, alertRule);
            case EnergyMeter em -> energyMeterAlertsManager.check(em, alertRule);
            case SmartLight sl -> smartLightAlertsManager.check(sl, alertRule);
            case SmartPlug sp -> smartPlugAlertsManager.check(sp, alertRule);
            case SoilMoistureSensor sms -> soilMoistureSensorAlertsManager.check(sms, alertRule);
            case TemperatureSensor ts -> temperatureSensorAlertManager.check(ts, alertRule);
            case Thermostat t -> thermostatAlertsManager.check(t, alertRule);
            default -> throw new IllegalArgumentException("Unknown telemetry type: " + telemetry);
        };
    }
}
