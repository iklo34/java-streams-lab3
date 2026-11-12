package ua.kpi.comsys.streams;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TelemetryStreamAnalyzerTest {

    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");
    private static final double EPS = 1e-9;

    private TelemetryStreamAnalyzer analyzer;

    @BeforeEach
    void setUp() {
        analyzer = new TelemetryStreamAnalyzer();
    }

    private static SensorMetric metric(int second, String deviceId, String type, double value) {
        return new SensorMetric(deviceId, type, value, START.plusSeconds(second));
    }

    /**
     * Нескінченний стрім: пристрої dev-0..dev-2 по черзі, тип temperature,
     * значення = номер елемента (0, 1, 2, ...).
     */
    private static Stream<SensorMetric> infiniteTelemetry() {
        return Stream.iterate(0, i -> i + 1)
                .map(i -> metric(i, "dev-" + (i % 3), "temperature", i));
    }

    @Nested
    @DisplayName("findFirstAnomaly")
    class FindFirstAnomaly {

        @Test
        @Timeout(2)
        void findsFirstAnomalyInInfiniteStream() {
            Optional<SensorMetric> result =
                    analyzer.findFirstAnomaly(infiniteTelemetry(), "dev-1", 100.0);

            // dev-1 отримує значення 1, 4, 7, ...; перше > 100 — це 103
            assertEquals(Optional.of(103.0), result.map(SensorMetric::value));
            assertEquals("dev-1", result.map(SensorMetric::deviceId).orElse("none"));
        }

        @Test
        @Timeout(2)
        void stopsConsumingInfiniteStreamRightAfterMatch() {
            AtomicInteger consumed = new AtomicInteger();
            Stream<SensorMetric> stream = infiniteTelemetry().peek(m -> consumed.incrementAndGet());

            analyzer.findFirstAnomaly(stream, "dev-2", 10.0);

            // dev-2 отримує 2, 5, 8, 11 -> збіг на елементі з індексом 11, тобто 12-му
            assertEquals(12, consumed.get());
        }

        @Test
        void thresholdIsStrict() {
            Stream<SensorMetric> stream = Stream.of(
                    metric(0, "dev-1", "temperature", 50.0),
                    metric(1, "dev-1", "temperature", 50.1));

            assertEquals(Optional.of(50.1),
                    analyzer.findFirstAnomaly(stream, "dev-1", 50.0).map(SensorMetric::value));
        }

        @Test
        void ignoresAnomaliesOfOtherDevices() {
            Stream<SensorMetric> stream = Stream.of(
                    metric(0, "dev-2", "temperature", 999.0),
                    metric(1, "dev-1", "temperature", 10.0),
                    metric(2, "dev-1", "temperature", 120.0));

            SensorMetric found = analyzer.findFirstAnomaly(stream, "dev-1", 100.0)
                    .orElseThrow();
            assertEquals(START.plusSeconds(2), found.timestamp());
        }

        @Test
        void returnsEmptyWhenFiniteStreamHasNoAnomaly() {
            Stream<SensorMetric> stream = Stream.of(
                    metric(0, "dev-1", "temperature", 10.0),
                    metric(1, "dev-2", "temperature", 500.0));

            assertTrue(analyzer.findFirstAnomaly(stream, "dev-1", 100.0).isEmpty());
        }

        @Test
        void returnsEmptyForEmptyStream() {
            assertTrue(analyzer.findFirstAnomaly(Stream.empty(), "dev-1", 0).isEmpty());
        }

        @Test
        void skipsNullElements() {
            Stream<SensorMetric> stream = Stream.of(null, metric(0, "dev-1", "t", 5.0));
            assertTrue(analyzer.findFirstAnomaly(stream, "dev-1", 1.0).isPresent());
        }

        @Test
        void nullStreamThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> analyzer.findFirstAnomaly(null, "dev-1", 1.0));
        }

        @Test
        void nullDeviceIdThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> analyzer.findFirstAnomaly(Stream.empty(), null, 1.0));
        }

        @Test
        void nanThresholdThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> analyzer.findFirstAnomaly(Stream.empty(), "dev-1", Double.NaN));
        }
    }

    @Nested
    @DisplayName("streamActiveMetrics")
    class StreamActiveMetrics {

        @Test
        @Timeout(2)
        void skipsStableHeadAndTakesNextMetricsFromInfiniteStream() {
            List<SensorMetric> result = analyzer.streamActiveMetrics(infiniteTelemetry(), 4.0, 3);

            assertIterableEquals(List.of(5.0, 6.0, 7.0),
                    result.stream().map(SensorMetric::value).toList());
        }

        @Test
        @Timeout(2)
        void keepsLaterStableMetricsOnceActivePhaseStarted() {
            // 0, 0, 0, 10, 0, 10, 0, ... — після першого 10 dropWhile більше нічого не відкидає
            AtomicInteger counter = new AtomicInteger();
            Stream<SensorMetric> stream = Stream.generate(() -> {
                int i = counter.getAndIncrement();
                double value = (i < 3) ? 0.0 : (i % 2 == 1 ? 10.0 : 0.0);
                return metric(i, "dev-1", "temperature", value);
            });

            List<Double> values = analyzer.streamActiveMetrics(stream, 5.0, 4)
                    .stream().map(SensorMetric::value).toList();

            assertIterableEquals(List.of(10.0, 0.0, 10.0, 0.0), values);
        }

        @Test
        void valueEqualToThresholdIsStable() {
            Stream<SensorMetric> stream = Stream.of(
                    metric(0, "dev-1", "t", 5.0),
                    metric(1, "dev-1", "t", 5.0),
                    metric(2, "dev-1", "t", 5.5));

            List<SensorMetric> result = analyzer.streamActiveMetrics(stream, 5.0, 10);
            assertEquals(1, result.size());
            assertEquals(5.5, result.getFirst().value(), EPS);
        }

        @Test
        void returnsFewerThanMaxCountForShortFiniteStream() {
            Stream<SensorMetric> stream = Stream.of(
                    metric(0, "dev-1", "t", 1.0),
                    metric(1, "dev-1", "t", 9.0),
                    metric(2, "dev-1", "t", 8.0));

            assertEquals(2, analyzer.streamActiveMetrics(stream, 2.0, 10).size());
        }

        @Test
        void returnsEmptyListWhenAllMetricsAreStable() {
            Stream<SensorMetric> stream = Stream.of(metric(0, "dev-1", "t", 1.0));
            assertTrue(analyzer.streamActiveMetrics(stream, 2.0, 5).isEmpty());
        }

        @Test
        void nullStreamThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> analyzer.streamActiveMetrics(null, 1.0, 1));
        }

        @ParameterizedTest
        @ValueSource(ints = {0, -1})
        void nonPositiveMaxCountThrows(int maxCount) {
            assertThrows(IllegalArgumentException.class,
                    () -> analyzer.streamActiveMetrics(Stream.empty(), 1.0, maxCount));
        }

        @Test
        void infiniteThresholdThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> analyzer.streamActiveMetrics(Stream.empty(), Double.POSITIVE_INFINITY, 1));
        }
    }

    @Nested
    @DisplayName("calculateMovingAverages (Gatherer)")
    class CalculateMovingAverages {

        @Test
        @Timeout(2)
        void computesMovingAverageOnInfiniteStream() {
            // значення 0, 1, 2, 3, 4, ...; вікно 3
            List<Double> result = analyzer.calculateMovingAverages(infiniteTelemetry(), "temperature", 3, 5);

            assertDoublesEqual(List.of(0.0, 0.5, 1.0, 2.0, 3.0), result);
        }

        @Test
        @Timeout(2)
        void considersOnlyRequestedMetricType() {
            // чергуються temperature (10, 20, 30, ...) та humidity (999)
            AtomicInteger counter = new AtomicInteger();
            Stream<SensorMetric> stream = Stream.generate(() -> {
                int i = counter.getAndIncrement();
                return i % 2 == 0
                        ? metric(i, "dev-1", "temperature", (i / 2 + 1) * 10.0)
                        : metric(i, "dev-1", "humidity", 999.0);
            });

            List<Double> result = analyzer.calculateMovingAverages(stream, "temperature", 2, 4);

            assertDoublesEqual(List.of(10.0, 15.0, 25.0, 35.0), result);
        }

        @Test
        @Timeout(2)
        void windowOfOneReturnsOriginalValues() {
            Stream<SensorMetric> stream = Stream.iterate(1.0, v -> v * 2)
                    .map(v -> metric(0, "dev-1", "power", v));

            assertDoublesEqual(List.of(1.0, 2.0, 4.0, 8.0),
                    analyzer.calculateMovingAverages(stream, "power", 1, 4));
        }

        @Test
        @Timeout(2)
        void outputLimitStopsInfiniteStream() {
            AtomicInteger consumed = new AtomicInteger();
            Stream<SensorMetric> stream = infiniteTelemetry().peek(m -> consumed.incrementAndGet());

            List<Double> result = analyzer.calculateMovingAverages(stream, "temperature", 4, 100);

            assertEquals(100, result.size());
            assertTrue(consumed.get() < 1_000, "stream must not be consumed beyond what is needed");
        }

        @Test
        void returnsAllAvailableAveragesForShortFiniteStream() {
            Stream<SensorMetric> stream = IntStream.rangeClosed(1, 3)
                    .mapToObj(i -> metric(i, "dev-1", "temperature", i * 3.0));

            assertDoublesEqual(List.of(3.0, 4.5, 6.0),
                    analyzer.calculateMovingAverages(stream, "temperature", 5, 10));
        }

        @Test
        void returnsEmptyListWhenMetricTypeIsAbsent() {
            Stream<SensorMetric> stream = Stream.of(metric(0, "dev-1", "humidity", 40.0));
            assertTrue(analyzer.calculateMovingAverages(stream, "temperature", 2, 5).isEmpty());
        }

        @Test
        void skipsNullElements() {
            Stream<SensorMetric> stream = Stream.of(
                    metric(0, "dev-1", "t", 2.0), null, metric(1, "dev-1", "t", 4.0));

            assertDoublesEqual(List.of(2.0, 3.0), analyzer.calculateMovingAverages(stream, "t", 2, 5));
        }

        @Test
        void eachCallHasIndependentState() {
            List<Double> first = analyzer.calculateMovingAverages(infiniteTelemetry(), "temperature", 2, 3);
            List<Double> second = analyzer.calculateMovingAverages(infiniteTelemetry(), "temperature", 2, 3);
            assertEquals(first, second);
        }

        @Test
        void nullStreamThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> analyzer.calculateMovingAverages(null, "t", 1, 1));
        }

        @ParameterizedTest
        @ValueSource(strings = {"", " ", "\t"})
        void blankMetricTypeThrows(String metricType) {
            assertThrows(IllegalArgumentException.class,
                    () -> analyzer.calculateMovingAverages(Stream.empty(), metricType, 1, 1));
        }

        @Test
        void nullMetricTypeThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> analyzer.calculateMovingAverages(Stream.empty(), null, 1, 1));
        }

        @ParameterizedTest
        @ValueSource(ints = {0, -3})
        void nonPositiveWindowSizeThrows(int windowSize) {
            assertThrows(IllegalArgumentException.class,
                    () -> analyzer.calculateMovingAverages(Stream.empty(), "t", windowSize, 1));
        }

        @ParameterizedTest
        @ValueSource(ints = {0, -3})
        void nonPositiveOutputLimitThrows(int outputLimit) {
            assertThrows(IllegalArgumentException.class,
                    () -> analyzer.calculateMovingAverages(Stream.empty(), "t", 1, outputLimit));
        }

        private static void assertDoublesEqual(List<Double> expected, List<Double> actual) {
            assertEquals(expected.size(), actual.size(), "size");
            for (int i = 0; i < expected.size(); i++) {
                assertEquals(expected.get(i), actual.get(i), EPS, "index " + i);
            }
        }
    }
}
