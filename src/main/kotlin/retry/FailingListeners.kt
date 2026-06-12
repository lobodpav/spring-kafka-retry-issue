package retry

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

@Component
class FailingListeners {
    val blockingDeliveries = AtomicInteger()
    val suspendDeliveries = AtomicInteger()

    /** Per-record delivery counts for the two-record interleave scenarios, keyed by record value. */
    val blockingInterleaveDeliveries = ConcurrentHashMap<String, Int>()
    val suspendInterleaveDeliveries = ConcurrentHashMap<String, Int>()

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

    @KafkaListener(topics = ["blocking-interleave-topic"], groupId = "blocking-interleave-group-id")
    fun onBlockingInterleave(record: ConsumerRecord<String, String>) {
        blockingInterleaveDeliveries.merge(record.value(), 1, Int::plus)
        throw RuntimeException("Always fail (blocking interleave)")
    }

    @KafkaListener(topics = ["suspend-interleave-topic"], groupId = "suspend-interleave-group-id")
    suspend fun onSuspendInterleave(record: ConsumerRecord<String, String>) {
        suspendInterleaveDeliveries.merge(record.value(), 1, Int::plus)
        throw RuntimeException("Always fail (suspend interleave)")
    }
}
