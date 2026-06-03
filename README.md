# Suspend `@KafkaListener` re-delivers a failing record without bound after `DefaultErrorHandler` retries are exhausted

### In what version(s) of Spring for Apache Kafka are you seeing this issue?

`4.1.0-M2` and `4.1.0-RC1` (with Spring Boot `4.1.0-M2` / `4.1.0-RC1`).
Not reproducible on `4.0.x` (see *Additional observations* below).

### Describe the bug

With a **non-transactional** container and a Kotlin **`suspend` `@KafkaListener`** (any async return type behaves the same) configured with a `DefaultErrorHandler(FixedBackOff(interval, n))`, a record whose processing always fails is **re-delivered without bound**.

A **blocking** (non-suspend) listener with the identical configuration behaves as expected: it is delivered `n + 1` times (initial delivery + `n` retries) and then stops.

For the `suspend` listener the delivery count keeps growing indefinitely. With a `DeadLetterPublishingRecoverer`, a new DLT copy is published on every cycle.

### Expected behavior

The `suspend`/async listener should behave like the blocking one: it is delivered `n + 1` times and then stops; the record is not re-delivered after retries are exhausted.

### Actual behavior

The record is re-delivered forever.

With `FixedBackOff(100L, 2L)` (initial delivery + 2 retries → expected 3 deliveries), and one record sent to each topic:

- `blockingDeliveries` settles at **3** and stops.
- `suspendDeliveries` keeps climbing (≈18–20 within 10s at a 100 ms back off) and never settles.

The consumer **offset is committed** for the suspend group. Reading it straight from the broker (`AdminClient.listConsumerGroupOffsets`) once per second shows it reaches `1` (past the single record at offset `0`) within the first second and stays `1`, while the delivery counter keeps growing:

```
t=1s  suspendDeliveries=2   committedOffset=1
t=2s  suspendDeliveries=5   committedOffset=1
...
t=10s suspendDeliveries=21  committedOffset=1
```

Container `DEBUG` logs (`org.springframework.kafka.listener=DEBUG`) for the suspend topic, after retries are exhausted:

```
Backoff FixedBackOffExecution[interval=100, currentAttempts=3, maxAttempts=2] exhausted for suspend-topic-0@0
Skipping seek of: suspend-topic-0@0
Seeking to offset 0 for partition suspend-topic-0
Seeking to offset 0 for partition suspend-topic-0
Backoff ... exhausted for suspend-topic-0@0
Skipping seek of: suspend-topic-0@0
Seeking to offset 0 for partition suspend-topic-0
... (repeats)
```

The equivalent blocking topic logs `Skipping seek of: blocking-topic-0@0` once and then produces no further `Seeking to offset 0` lines.

### To Reproduce

Minimal project: **https://github.com/lobodpav/spring-kafka-retry-issue**. No transactions, no `@RetryableTopic`, no custom recoverer — just Spring Boot + spring-kafka + a Testcontainers broker, with one blocking and one `suspend` `@KafkaListener` that always throw, and a `DefaultErrorHandler(FixedBackOff(100L, 2L))`.

Run `./gradlew test`. One record is sent to each topic; the test asserts each listener is delivered 3 times. The blocking assertion passes; the `suspend` assertion fails because `suspendDeliveries` keeps growing.

### Additional observations

- On `4.0.x` the same `suspend` listener is delivered **once and is not retried at all**; the unbounded re-delivery appears on `4.1.0-M2` / `4.1.0-RC1`.
- `errorHandler.setCommitRecovered(true)`, with or without `spring.kafka.listener.ack-mode=MANUAL_IMMEDIATE`, does not change the behaviour — the suspend listener still loops.
