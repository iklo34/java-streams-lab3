package ua.kpi.comsys.streams;

import java.time.Instant;

/**
 * Одне вимірювання з IoT-пристрою.
 *
 * @param deviceId   ідентифікатор пристрою
 * @param metricType тип метрики (наприклад, "temperature", "humidity")
 * @param value      виміряне значення
 * @param timestamp  момент вимірювання
 */
public record SensorMetric(String deviceId, String metricType, double value, Instant timestamp) {}
