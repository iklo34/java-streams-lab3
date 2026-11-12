package ua.kpi.comsys.streams;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.stream.Gatherer;

/**
 * Кастомна проміжна операція (Stream Gatherer), що обчислює ковзне середнє
 * значень метрики заданого типу.
 *
 * <p>Gatherer послідовний і зберігає стан — вікно з останніх {@code windowSize}
 * значень та їхню поточну суму. Для кожної нової метрики потрібного типу значення
 * додається у вікно; якщо вікно переповнене, найстаріше значення видаляється.
 * Після цього вниз по стріму передається середнє значень, що зараз у вікні.
 * Поки вікно не заповнене повністю, середнє рахується за наявними значеннями.
 *
 * <p>Метрики іншого типу та {@code null}-елементи пропускаються, не змінюючи стан.
 * Операція лінива: якщо наступна стадія (наприклад, {@code limit}) більше не приймає
 * елементи, обробка нескінченного стріму зупиняється.
 */
final class MovingAverageGatherer {

    private MovingAverageGatherer() {
    }

    static Gatherer<SensorMetric, ?, Double> of(String metricType, int windowSize) {
        return Gatherer.ofSequential(
                () -> new Window(windowSize),
                Gatherer.Integrator.ofGreedy((window, metric, downstream) -> {
                    if (metric == null || !metricType.equals(metric.metricType())) {
                        return !downstream.isRejecting();
                    }
                    return downstream.push(window.add(metric.value()));
                })
        );
    }

    /** Стан Gatherer: вікно останніх значень і їхня сума. */
    private static final class Window {
        private final int capacity;
        private final Deque<Double> values;
        private double sum;

        Window(int capacity) {
            this.capacity = capacity;
            this.values = new ArrayDeque<>(capacity);
        }

        double add(double value) {
            values.addLast(value);
            sum += value;
            if (values.size() > capacity) {
                sum -= values.removeFirst();
            }
            return sum / values.size();
        }
    }
}
