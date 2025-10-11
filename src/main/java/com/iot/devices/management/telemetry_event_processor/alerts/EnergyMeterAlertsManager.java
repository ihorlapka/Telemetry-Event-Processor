package com.iot.devices.management.telemetry_event_processor.alerts;

import com.iot.alerts.Alert;
import com.iot.alerts.AlertRule;
import com.iot.alerts.ThresholdType;
import com.iot.devices.EnergyMeter;
import lombok.RequiredArgsConstructor;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Optional;

import static com.iot.alerts.ThresholdType.GREATER_THAN;
import static com.iot.alerts.ThresholdType.LESS_THAN;
import static java.util.Optional.empty;

@Component
@RequiredArgsConstructor
public class EnergyMeterAlertsManager implements AlertsManager<EnergyMeter> {

    @Override
    public Optional<Alert> checkEnergyMeter(EnergyMeter em, AlertRule alertRule) {
        return switch (alertRule.getMetricName()) {
            case VOLTAGE -> check(alertRule, em.getVoltage());
            case CURRENT -> check(alertRule, em.getCurrent());
            case POWER -> check(alertRule, em.getPower());
            case ENERGY_CONSUMED -> check(alertRule, em.getEnergyConsumed());
            default -> throw new IllegalArgumentException("Unable to check " + alertRule.getMetricName() + " for " + em);
        };
    }

    private Optional<Alert> check(AlertRule alertRule, @Nullable Float param) {
        if (param != null && (checkThreshold(LESS_THAN, param, alertRule) || checkThreshold(GREATER_THAN, param, alertRule))) {
            return createAlert(alertRule, param);
        }
        return empty();
    }
}
