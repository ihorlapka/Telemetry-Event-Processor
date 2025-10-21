package com.iot.devices.management.telemetry_event_processor.alerts;

import com.iot.alerts.Alert;
import com.iot.alerts.AlertRule;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.lang.Nullable;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Optional.empty;
import static java.util.Optional.of;

public interface AlertsManager<T extends SpecificRecord> {

    Optional<Alert> check(T telemetry, AlertRule alertRule);

    default Optional<Alert> checkThreshold(String deviceId, AlertRule alertRule, @Nullable Float param) {
        if (param != null && isThresholdReached(param, alertRule)) {
            return createAlert(deviceId, alertRule, param);
        }
        return empty();
    }

    default Optional<Alert> checkBattery(String deviceId, AlertRule alertRule, @Nullable Integer batteryLevel) {
        if (batteryLevel != null && isThresholdReached(batteryLevel, alertRule)) {
            return createAlert(deviceId, alertRule, (float) batteryLevel);
        }
        return empty();
    }

    default Optional<Alert> checkTimeThreshold(String deviceId, @Nullable Instant telemetryTime, AlertRule alertRule) {
        if (telemetryTime == null || alertRule.getThresholdValue() == null) {
            return empty();
        }
        if (hasTimeExpired(telemetryTime, alertRule)) {
            return createAlert(deviceId, alertRule, (float) telemetryTime.toEpochMilli());
        }
        return empty();
    }

    default Optional<Alert> createAlert(String deviceId, AlertRule alertRule, @Nullable Float value) {
        return of(Alert.newBuilder()
                .setAlertId(UUID.randomUUID().toString())
                .setDeviceId(deviceId)
                .setRuleId(alertRule.getRuleId())
                .setSeverity(alertRule.getSeverity())
                .setTimestamp(Instant.now())
                .setMessage(createMessage(alertRule))
                .setActualValue(value)
                .build());
    }

    private boolean isThresholdReached(float telemetryValue, AlertRule alertRule) {
        final int compare = BigDecimal.valueOf(telemetryValue).compareTo(BigDecimal.valueOf(alertRule.getThresholdValue()));
        return switch (alertRule.getThresholdType()) {
            case GREATER_THAN -> compare > 0;
            case LESS_THAN -> compare < 0;
            default -> throw new IllegalArgumentException("Unable to check threshold because threshold type is unknown");
        };
    }

    private boolean hasTimeExpired(Instant telemetryTime, AlertRule alertRule) {
        return telemetryTime.isAfter(Instant.now().plus(Duration.of(alertRule.getThresholdValue().longValue(), SECONDS)));
    }

    private String createMessage(AlertRule alertRule) {
        return "The value of %s is %s, defined normal threshold %s"
                .formatted(alertRule.getMetricName().name().toLowerCase().replaceAll("_", " "),
                        alertRule.getThresholdType().name().toLowerCase().replaceAll("_", " "),
                        alertRule.getThresholdValue());
    }
}
