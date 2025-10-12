package com.iot.devices.management.telemetry_event_processor.alerts;

import com.iot.alerts.Alert;
import com.iot.alerts.AlertRule;
import com.iot.devices.EnergyMeter;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@RequiredArgsConstructor
public class EnergyMeterAlertsManager implements AlertsManager<EnergyMeter> {

    @Override
    public Optional<Alert> check(EnergyMeter em, AlertRule alertRule) {
        return switch (alertRule.getMetricName()) {
            case VOLTAGE -> checkThreshold(alertRule, em.getVoltage());
            case CURRENT -> checkThreshold(alertRule, em.getCurrent());
            case POWER -> checkThreshold(alertRule, em.getPower());
            case ENERGY_CONSUMED -> checkThreshold(alertRule, em.getEnergyConsumed());
            default -> throw new IllegalArgumentException("Unable to check " + alertRule.getMetricName() + " for " + em);
        };
    }
}
