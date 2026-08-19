package com.agent.stream.listener

import com.agent.stream.dto.AgentEvent
import com.agent.stream.service.StreamService
import com.fasterxml.jackson.databind.ObjectMapper
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

private val logger = KotlinLogging.logger {}

/**
 * Kafka 'agent-events' 토픽으로부터 AgentEvent 메시지를 수신하는 리스너 컴포넌트입니다.
 */
@Component
class KafkaEventListener(
    private val streamService: StreamService,
    private val objectMapper: ObjectMapper
) {

    @KafkaListener(
        topics = ["\${app.kafka.topic-events:agent-events}"],
        groupId = "\${spring.kafka.consumer.group-id:agent-stream-server-group}"
    )
    fun onMessage(message: String) {
        try {
            val event = objectMapper.readValue(message, AgentEvent::class.java)
            logger.debug { "Kafka AgentEvent 수신: type=${event.type}, commandId=${event.commandId}, eventId=${event.eventId}" }

            // 분산 이벤트 라우팅 핸들러로 전달
            streamService.handleAgentEvent(event)
        } catch (e: Exception) {
            logger.error(e) { "Kafka AgentEvent 메시지 파싱 오류 발생: $message" }
        }
    }
}
