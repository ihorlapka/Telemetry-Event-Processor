package com.iot.devices.management.telemetry_event_processor.alerts;

import com.iot.alerts.Alert;
import com.iot.alerts.AlertRule;
import com.iot.alerts.MetricType;
import com.iot.devices.*;
import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.stereotype.Component;

import java.util.*;

import static java.util.stream.Collectors.groupingBy;

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
        final Map<MetricType, List<AlertRule>> alertRulesByMetricType = alertRules.stream()
                .collect(groupingBy(AlertRule::getMetricName));
        final List<Alert> alerts = new ArrayList<>();
        for (Map.Entry<MetricType, List<AlertRule>> entry : alertRulesByMetricType.entrySet()) {
            final List<Alert> filteredAlerts = new ArrayList<>();
            for (AlertRule alertRule : entry.getValue()) {
                verify(telemetry, alertRule).ifPresent(filteredAlerts::add);
            }
            filteredAlerts.stream()
                    .max(Comparator.comparingInt(alert -> alert.getSeverity().ordinal()))
                    .ifPresent(alerts::add);
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
