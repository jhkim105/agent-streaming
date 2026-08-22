package com.agent.stream.config

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.NewTopic
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder
import java.util.UUID

private val logger = KotlinLogging.logger {}

@Configuration
class AgentStreamConfig {

    @Value("\${app.kafka.topic-commands:agent-commands}")
    private lateinit var topicCommands: String

    @Value("\${app.kafka.topic-events:agent-events}")
    private lateinit var topicEvents: String

    /**
     * 서버 인스턴스가 뜰 때 부여받는 유니크한 UUID Host ID입니다.
     */
    @Bean
    fun hostId(): String {
        val uniqueHostId = "kotlin-node-" + UUID.randomUUID().toString().take(8)
        logger.info { "Agent Stream Server Host ID 할당 완료: $uniqueHostId" }
        return uniqueHostId
    }

    /**
     * Kafka agent-commands 토픽을 자동 생성합니다.
     */
    @Bean
    fun topicCommands(): NewTopic {
        logger.info { "Kafka 토픽 생성/확인: $topicCommands" }
        return TopicBuilder.name(topicCommands)
            .partitions(3)
            .replicas(1)
            .build()
    }

    /**
     * Kafka agent-events 토픽을 자동 생성합니다.
     */
    @Bean
    fun topicEvents(): NewTopic {
        logger.info { "Kafka 토픽 생성/확인: $topicEvents" }
        return TopicBuilder.name(topicEvents)
            .partitions(3)
            .replicas(1)
            .build()
    }
}
