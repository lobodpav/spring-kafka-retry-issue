package retry

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.test.context.TestConstructor
import kotlin.use

@SpringBootTest
@Import(KafkaContainerConfig::class)
@TestConstructor(autowireMode = TestConstructor.AutowireMode.ALL)
class KafkaRetryIssueAppTest(
    val template: KafkaTemplate<String, String>,
    val listeners: FailingListeners,
    val recoveryTracker: RecoveryTracker,
    val kafkaAdmin: KafkaAdmin,
) {

    /**
     * Fixed in `spring-kafka 4.1.0` by [PR 4469](https://github.com/spring-projects/spring-kafka/pull/4469).
     * The suspend listener used to be re-delivered without bound after retries were exhausted.
     */
    @Test
    fun `Single failing record is delivered initial + retries times for both listener kinds`() {
        // when: One record is sent to each topic
        template.send("blocking-topic", "v1")
        template.send("suspend-topic", "v2")

        // and: The listeners are given time to retry
        Thread.sleep(5_000)

        // then: The blocking listener stops after the initial delivery + 2 retries, and the record is recovered once
        assertEquals(3, listeners.blockingDeliveries.get())
        assertEquals(1, recoveryTracker.recoveries["blocking-topic/v1"])

        // and: The suspend listener behaves the same
        assertEquals(3, listeners.suspendDeliveries.get())
        assertEquals(1, recoveryTracker.recoveries["suspend-topic/v2"])
    }

    @Test
    fun `Blocking listener with two failing records on the same partition retries and recovers each in offset order`() {
        // when: Two records are sent back-to-back to the same (single) partition
        template.send("blocking-interleave-topic", "r1")
        template.send("blocking-interleave-topic", "r2")

        // and: The listener is given time to retry
        Thread.sleep(5_000)

        // then: Each record is delivered the initial time + 2 retries, and each is recovered exactly once
        assertAll(
            { assertEquals(3, listeners.blockingInterleaveDeliveries["r1"], "r1 deliveries") },
            { assertEquals(3, listeners.blockingInterleaveDeliveries["r2"], "r2 deliveries") },
            { assertEquals(1, recoveryTracker.recoveries["blocking-interleave-topic/r1"], "r1 recoveries") },
            { assertEquals(1, recoveryTracker.recoveries["blocking-interleave-topic/r2"], "r2 recoveries") },
        )
    }

    /**
     * FAILS on `spring-kafka 4.1.0`
     * - r1 is delivered once, never retried, and never recovered, yet the committed offset moves past it - the record is **silently lost**.
     * - r2 retries and recovers normally.
     */
    @Test
    fun `Suspend listener with two failing records on the same partition retries and recovers each like the blocking listener`() {
        // when: Two records are sent back-to-back to the same (single) partition
        template.send("suspend-interleave-topic", "r1")
        template.send("suspend-interleave-topic", "r2")

        // and: The listener is given time to retry
        Thread.sleep(5_000)

        // then: Each record is delivered the initial time + 2 retries, and each is recovered exactly once.
        //       The committed offset moves past both records either way - which is only correct if both were recovered.
        assertAll(
            { assertEquals(3, listeners.suspendInterleaveDeliveries["r1"], "r1 deliveries") },
            { assertEquals(3, listeners.suspendInterleaveDeliveries["r2"], "r2 deliveries") },
            { assertEquals(1, recoveryTracker.recoveries["suspend-interleave-topic/r1"], "r1 recoveries") },
            { assertEquals(1, recoveryTracker.recoveries["suspend-interleave-topic/r2"], "r2 recoveries") },
            { assertEquals(2L, committedOffset("suspend-interleave-group-id", "suspend-interleave-topic"), "committed offset") },
        )
    }

    /** Reads the group's committed offset for the topic's single partition straight from the broker. */
    private fun committedOffset(groupId: String, topic: String): Long? =
        AdminClient.create(kafkaAdmin.configurationProperties).use { client ->
            client.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata().get()[TopicPartition(topic, 0)]?.offset()
        }
}
