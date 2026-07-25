package com.agent.stream.listener

import com.agent.stream.dto.AgentResponseEvent
import com.agent.stream.service.StreamService
import com.fasterxml.jackson.databind.ObjectMapper
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

private val logger = KotlinLogging.logger {}

@Component
class KafkaResponseListener(
    private val streamService: StreamService,
    private val objectMapper: ObjectMapper
) {

    @KafkaListener(
        topics = ["\${app.kafka.topic-responses:agent-responses}"],
        groupId = "\${spring.kafka.consumer.group-id:agent-stream-server-group}"
    )
    fun onMessage(message: String) {
        try {
            val event = objectMapper.readValue(message, AgentResponseEvent::class.java)
            logger.debug { "Kafka 응답 수신: type=${event.type}, hostId=${event.hostId}, sessionId=${event.sessionId}" }

            // 분산 세션 라우팅 핸들러로 전달
            streamService.handleAgentResponse(event)
        } catch (e: Exception) {
            logger.error(e) { "Kafka 응답 메시지 파싱 오류 발생: $message" }
        }
    }
}
