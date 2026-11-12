# Лабораторна робота №3 — Нескінченні стріми, Optional та Stream Gatherers

**Студент:** Бабело Андрій
**Тема:** Опрацювання нескінченних послідовностей даних за допомогою ледачих операцій Java Stream API,
безпечне керування наявністю значень через `Optional` та створення кастомних проміжних операцій
за допомогою Stream Gatherers.

## Опис проєкту

Сервіс аналітики телеметрії IoT-пристроїв (пакет `ua.kpi.comsys.streams`). Він приймає
потенційно **нескінченний** `Stream<SensorMetric>` і обробляє його лише ледачими операціями
та операціями короткого замикання. Завдяки цьому зі стріму читається тільки стільки елементів,
скільки потрібно для результату.

| Клас | Призначення |
|---|---|
| `SensorMetric` | `record SensorMetric(String deviceId, String metricType, double value, Instant timestamp)` |
| `TelemetryStreamAnalyzer` | публічний API сервісу |
| `MovingAverageGatherer` | кастомний `java.util.stream.Gatherer` для ковзного середнього |

### Методи `TelemetryStreamAnalyzer`

- **`findFirstAnomaly(stream, targetDeviceId, criticalThreshold)`** — шукає першу метрику пристрою
  `targetDeviceId`, у якої `value > criticalThreshold`. Конвеєр `filter → filter → findFirst`
  закінчується операцією короткого замикання, тож обробка нескінченного стріму зупиняється одразу
  після збігу. Результат повертається як `Optional`, а для скінченного стріму без збігу
  це `Optional.empty()`.
- **`streamActiveMetrics(stream, normalThreshold, maxCount)`** — `dropWhile` відкидає стабільні
  метрики на початку стріму (`value <= normalThreshold`), а `limit(maxCount)` бере не більше
  `maxCount` наступних. Після початку активної фази `dropWhile` більше нічого не відкидає.
- **`calculateMovingAverages(stream, metricType, windowSize, outputLimit)`** — застосовує
  `stream.gather(...)` з кастомним Gatherer і обмежує видачу через `limit(outputLimit)`.

### Кастомний Gatherer

`MovingAverageGatherer` створюється через `Gatherer.ofSequential(initializer, Integrator.ofGreedy(...))`.

- **Стан:** вікно `ArrayDeque<Double>` з останніми `windowSize` значеннями та їхня поточна сума.
- **Для кожного елемента:**
  - метрики іншого типу та `null` пропускаються, стан не змінюється;
  - для метрики потрібного типу значення додається у вікно, найстаріше видаляється при переповненні,
    і вниз по стріму передається `sum / size`.
- Поки вікно не заповнене, середнє рахується за наявними значеннями. Наприклад, для значень
  `0, 1, 2, 3, 4` і вікна 3 результат буде `0.0, 0.5, 1.0, 2.0, 3.0`.
- Gatherer повертає результат `downstream.push(...)`. Коли `limit` більше не приймає елементи,
  обробка нескінченного джерела припиняється.

### Валідація

Будь-яке з наведеного нижче кидає `IllegalArgumentException`:
- `stream`, `targetDeviceId` або `metricType` дорівнює `null`;
- `metricType` порожній чи складається лише з пробілів;
- `maxCount`, `windowSize` або `outputLimit` `<= 0`;
- поріг дорівнює `NaN` або нескінченності.

Перевірки на `null` зроблено через `Optional.ofNullable(...).orElseThrow(...)`.

> **Зауваження:** якщо в нескінченному стрімі ніколи не трапиться шуканий елемент
> (немає аномалії або всі метрики стабільні), обробка не може завершитися. Це властивість
> будь-якої операції пошуку на нескінченному джерелі, тому в тестах використовуються джерела,
> де результат гарантовано існує.

## Розрахунок варіанта

Залікової книжки немає, тому використано порядковий номер у списку групи: **N = 1**.

```
V = N mod 3 = 1 mod 3 = 1
```

**Варіант 1 — Телеметрія IoT-пристроїв у реальному часі.**

## Тести

`TelemetryStreamAnalyzerTest` (JUnit 5) перевіряє:
- кожен метод на **нескінченних** стрімах (`Stream.iterate`, `Stream.generate`) з `@Timeout(2)`;
- лінивість: через `peek` рахується кількість прочитаних елементів, і `findFirstAnomaly`
  читає рівно стільки, скільки потрібно до першого збігу;
- граничні випадки: строгий поріг, значення рівне порогу, порожній та короткий скінченний стрім,
  відсутній тип метрики, `null`-елементи, незалежний стан Gatherer між викликами;
- усі випадки `IllegalArgumentException`.

## Збірка та запуск тестів

Stream Gatherers стабільні з **Java 24**, тому проєкт компілюється під **Java 25** (LTS).
Встановлювати Gradle не треба, його завантажить Gradle Wrapper. JDK 25 теж завантажиться
автоматично (плагін `foojay-resolver-convention`), якщо на комп'ютері є лише старіша Java
(мінімум 17, щоб запустити сам Gradle).

Linux / macOS:
```bash
./gradlew build
./gradlew test
```

Windows (PowerShell):
```powershell
.\gradlew.bat build
.\gradlew.bat test
```

Звіт про тести: `build/reports/tests/test/index.html`
Звіт про покриття (JaCoCo): `build/reports/jacoco/test/html/index.html`
