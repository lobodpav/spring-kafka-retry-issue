package retry

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.stereotype.Component
import org.springframework.util.backoff.FixedBackOff
import java.util.concurrent.ConcurrentHashMap

/** Per-record recovery counts, keyed by `topic/value`. No entry means the record was never handed to the recoverer. */
@Component
class RecoveryTracker {
    val recoveries = ConcurrentHashMap<String, Int>()
}

@SpringBootApplication
class KafkaRetryIssueApp {
    // Initial delivery + 2 retries - each record should reach the listener at most 3 times, then be recovered exactly once.
    @Bean
    fun errorHandler(recoveryTracker: RecoveryTracker) = DefaultErrorHandler(
        { record, _ -> recoveryTracker.recoveries.merge("${record.topic()}/${record.value()}", 1, Int::plus) },
        FixedBackOff(100L, 2L),
    )
}
