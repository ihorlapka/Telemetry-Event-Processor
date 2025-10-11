package com.iot.devices.management.telemetry_event_processor.alerts;

import com.iot.alerts.Alert;
import com.iot.alerts.AlertRule;
import com.iot.devices.*;
import org.apache.avro.specific.SpecificRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Optional.empty;


public class AlertManagerProvider {

//    private final SpecificRecord telemetry;
//    private final List<AlertRule> alertRules;

    public List<Alert> createAlert(SpecificRecord telemetry, List<AlertRule> alertRules) {
        final List<Alert> alerts = new ArrayList<>();
        for (AlertRule alertRule : alertRules) {
            verify(telemetry, alertRule).ifPresent(alerts::add);
        }
        if (alerts.isEmpty()) {
            return null; //in order to skip message to be sent to alerts topic
        }
        return alerts;
    }

    private Optional<Alert> verify(SpecificRecord telemetry, AlertRule alertRule) {
        return switch (telemetry) {
//            case DoorSensor ds -> checkDoorSensor(ds, alertRule);
//            case EnergyMeter em -> checkEnergyMeter(em, alertRule);
//            case SmartLight sl -> checkSmartLight(sl, alertRule);
//            case SmartPlug sp -> checkSmartPlug(sp, alertRule);
//            case SoilMoistureSensor sms -> checkSoilMoistureSensor(sms, alertRule);
//            case TemperatureSensor ts -> checkTemperatureSensor(ts, alertRule);
//            case Thermostat t -> checkThermostat(t, alertRule);
            default -> throw new IllegalArgumentException("Unknown telemetry type: " + telemetry);
        };
    }


}
