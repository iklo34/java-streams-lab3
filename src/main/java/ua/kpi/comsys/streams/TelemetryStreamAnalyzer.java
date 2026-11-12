package ua.kpi.comsys.streams;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Аналітика телеметрії IoT-пристроїв на (потенційно нескінченних) стрімах.
 *
 * <p>Усі методи будують ліниві конвеєри й завершуються операціями короткого замикання
 * ({@code findFirst}, {@code limit}), тому на нескінченному джерелі обробляється
 * лише стільки елементів, скільки потрібно для результату.
 */
public class TelemetryStreamAnalyzer {

    /**
     * Знаходить першу метрику пристрою {@code targetDeviceId},
     * значення якої строго більше за {@code criticalThreshold}.
     *
     * @return знайдена метрика або {@link Optional#empty()}, якщо скінченний стрім її не містить
     */
    public Optional<SensorMetric> findFirstAnomaly(Stream<SensorMetric> stream,
                                                   String targetDeviceId,
                                                   double criticalThreshold) {
        requireNonNull(stream, "stream");
        requireNonNull(targetDeviceId, "targetDeviceId");
        requireFinite(criticalThreshold, "criticalThreshold");

        return stream
                .filter(Objects::nonNull)
                .filter(metric -> targetDeviceId.equals(metric.deviceId()))
                .filter(metric -> metric.value() > criticalThreshold)
                .findFirst();
    }

    /**
     * Пропускає стабільні метрики на початку стріму (значення {@code <= normalThreshold})
     * і повертає не більше {@code maxCount} наступних метрик.
     */
    public List<SensorMetric> streamActiveMetrics(Stream<SensorMetric> stream,
                                                  double normalThreshold,
                                                  int maxCount) {
        requireNonNull(stream, "stream");
        requireFinite(normalThreshold, "normalThreshold");
        requirePositive(maxCount, "maxCount");

        return stream
                .filter(Objects::nonNull)
                .dropWhile(metric -> metric.value() <= normalThreshold)
                .limit(maxCount)
                .toList();
    }

    /**
     * Обчислює ковзне середнє значень метрики {@code metricType} з вікном {@code windowSize}
     * за допомогою кастомного {@link java.util.stream.Gatherer} і повертає перші
     * {@code outputLimit} результатів.
     */
    public List<Double> calculateMovingAverages(Stream<SensorMetric> stream,
                                                String metricType,
                                                int windowSize,
                                                int outputLimit) {
        requireNonNull(stream, "stream");
        requireNonBlank(metricType, "metricType");
        requirePositive(windowSize, "windowSize");
        requirePositive(outputLimit, "outputLimit");

        return stream
                .gather(MovingAverageGatherer.of(metricType, windowSize))
                .limit(outputLimit)
                .toList();
    }

    private static <T> T requireNonNull(T value, String name) {
        return Optional.ofNullable(value)
                .orElseThrow(() -> new IllegalArgumentException(name + " cannot be null"));
    }

    private static void requireNonBlank(String value, String name) {
        requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " cannot be blank");
        }
    }

    private static void requirePositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be greater than 0");
        }
    }

    private static void requireFinite(double value, String name) {
        if (!Double.isFinite(value)) {
            throw new IllegalArgumentException(name + " must be a finite number");
        }
    }
}
