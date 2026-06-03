package retry

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicInteger

@Component
class FailingListeners {
    val blockingDeliveries = AtomicInteger()
    val suspendDeliveries = AtomicInteger()

    @KafkaListener(topics = ["blocking-topic"], groupId = "blocking-group-id")
    fun onBlocking(record: ConsumerRecord<String, String>) {
        blockingDeliveries.incrementAndGet()
        throw RuntimeException("Always fail (blocking)")
    }

    @KafkaListener(topics = ["suspend-topic"], groupId = "suspend-group-id")
    suspend fun onSuspend(record: ConsumerRecord<String, String>) {
        suspendDeliveries.incrementAndGet()
        throw RuntimeException("Always fail (suspend)")
    }
}
