package com.iot.devices.management.telemetry_event_processor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.Arrays;

@SpringBootApplication
public class TelemetryEventProcessorApplication {

	public static void main(String[] args) {
		ConfigurableApplicationContext context = SpringApplication.run(TelemetryEventProcessorApplication.class, args);
		System.out.println(Arrays.toString(context.getBeanDefinitionNames()));
	}

}
