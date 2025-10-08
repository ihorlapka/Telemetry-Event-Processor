package com.iot.devices.management.telemetry_event_processor;

import org.springframework.boot.SpringApplication;

public class TestTelemetryEventProcessorApplication {

	public static void main(String[] args) {
		SpringApplication.from(TelemetryEventProcessorApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
