package retry

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.test.context.TestConstructor

@SpringBootTest
@Import(KafkaContainerConfig::class)
@TestConstructor(autowireMode = TestConstructor.AutowireMode.ALL)
class KafkaRetryIssueAppTest(
    val template: KafkaTemplate<String, String>,
    val listeners: FailingListeners,
) {

    @Test
    fun `Suspend listener is re-delivered far more than the configured retries`() {
        // when: One record is sent to each topic
        template.send("blocking-topic", "v1")
        template.send("suspend-topic", "v2")

        // and: The listeners are given time to retry
        Thread.sleep(5_000)

        // then: The blocking listener stops after the initial delivery + 2 retries
        assertEquals(3, listeners.blockingDeliveries.get())

        // and: The suspend listener should stop after 3 too, but is re-delivered without bound (FAILS on 4.1.x)
        assertEquals(3, listeners.suspendDeliveries.get())
    }
}
