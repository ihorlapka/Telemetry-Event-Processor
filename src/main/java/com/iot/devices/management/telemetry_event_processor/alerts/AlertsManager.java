package com.iot.devices.management.telemetry_event_processor.alerts;

import com.iot.alerts.Alert;
import com.iot.alerts.AlertRule;
import com.iot.alerts.ThresholdType;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.lang.Nullable;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Optional.of;

public interface AlertsManager<T extends SpecificRecord> {

    Optional<Alert> checkEnergyMeter(T telemetry, AlertRule alertRule);

    default boolean checkThreshold(ThresholdType thresholdType, float telemetryValue, AlertRule alertRule) {
        final int compare = BigDecimal.valueOf(telemetryValue).compareTo(BigDecimal.valueOf(alertRule.getThresholdValue()));
        return switch (thresholdType) {
            case GREATER_THAN -> compare > 0;
            case LESS_THAN -> compare < 0;
            default -> throw new IllegalArgumentException("Unable to check threshold because threshold type is unknown");
        };
    }

    default boolean checkTimeThreshold(Instant telemetryTime, AlertRule alertRule) {
        if (telemetryTime == null || alertRule.getThresholdValue() == null) {
            return false;
        }
        return telemetryTime.isAfter(Instant.now().plus(Duration.of(alertRule.getThresholdValue().longValue(), SECONDS)));
    }

    default Optional<Alert> createAlert(AlertRule alertRule, @Nullable Float value) {
        return of(Alert.newBuilder()
                .setAlertId(UUID.randomUUID().toString())
                .setDeviceId(alertRule.getDeviceId())
                .setRuleId(alertRule.getRuleId())
                .setSeverity(alertRule.getSeverity())
                .setTimestamp(Instant.now())
                .setMessage(createMessage(alertRule))
                .setActualValue(value)
                .build());
    }

    default String createMessage(AlertRule alertRule) {
        return "The value of %s is %s, defined normal threshold %s"
                .formatted(alertRule.getMetricName().name().toLowerCase().replaceAll("_", " "),
                        alertRule.getThresholdType().name().toLowerCase().replaceAll("_", " "),
                        alertRule.getThresholdValue());
    }
}
