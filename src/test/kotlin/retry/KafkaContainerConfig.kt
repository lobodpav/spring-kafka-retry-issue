package retry

import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Bean
import org.testcontainers.kafka.ConfluentKafkaContainer

@TestConfiguration(proxyBeanMethods = false)
class KafkaContainerConfig {

    @Bean
    @ServiceConnection
    fun kafkaContainer(): ConfluentKafkaContainer = ConfluentKafkaContainer("confluentinc/cp-kafka:8.1.0")
}
