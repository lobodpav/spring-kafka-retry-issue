package retry

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.util.backoff.FixedBackOff

@SpringBootApplication
class KafkaRetryIssueApp {
    // Initial delivery + 2 retries - each record should reach the listener at most 3 times.
    @Bean
    fun errorHandler() = DefaultErrorHandler(FixedBackOff(100L, 2L))
}
