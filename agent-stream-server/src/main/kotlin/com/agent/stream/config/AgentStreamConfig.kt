package com.agent.stream.config

import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.UUID

private val logger = KotlinLogging.logger {}

@Configuration
class AgentStreamConfig {

    /**
     * 서버 인스턴스가 뜰 때 부여받는 유니크한 UUID Host ID입니다.
     */
    @Bean
    fun hostId(): String {
        val uniqueHostId = "kotlin-node-" + UUID.randomUUID().toString().take(8)
        logger.info { "Agent Stream Server Host ID 할당 완료: $uniqueHostId" }
        return uniqueHostId
    }
}
