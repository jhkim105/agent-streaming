package com.agent.stream.service

import com.agent.stream.dto.AgentResponseEvent
import com.agent.stream.session.SessionRegistry
import com.fasterxml.jackson.databind.ObjectMapper
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.codec.ServerSentEvent
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service

private val logger = KotlinLogging.logger {}

@Service
class StreamService(
    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val sessionRegistry: SessionRegistry,
    private val redisRoutingService: RedisRoutingService,
    private val hostId: String,
    private val objectMapper: ObjectMapper
) {
    @Value("\${app.kafka.topic-requests:agent-requests}")
    private lateinit var topicRequests: String

    /**
     * 질문을 Host ID 및 Session ID와 함께 카프카 요청 토픽으로 전송합니다.
     */
    fun sendUserQuery(sessionId: String, query: String) {
        val payload = mapOf(
            "sessionId" to sessionId,
            "hostId" to hostId,
            "query" to query
        )
        val jsonPayload = objectMapper.writeValueAsString(payload)

        logger.info { "Kafka 요청 토픽 전송 ($topicRequests): sessionId=$sessionId, hostId=$hostId" }
        kafkaTemplate.send(topicRequests, sessionId, jsonPayload)
    }

    /**
     * 사용자의 A2UI 액션을 Host ID 및 Session ID와 함께 카프카 요청 토픽으로 전송합니다.
     */
    fun sendUserAction(sessionId: String, actionId: String, payload: Map<String, Any>) {
        val messagePayload = mapOf(
            "sessionId" to sessionId,
            "hostId" to hostId,
            "query" to "A2UI_ACTION:$actionId",
            "actionId" to actionId,
            "payload" to payload
        )
        val jsonPayload = objectMapper.writeValueAsString(messagePayload)

        logger.info { "Kafka A2UI Action 전송 ($topicRequests): sessionId=$sessionId, actionId=$actionId" }
        kafkaTemplate.send(topicRequests, sessionId, jsonPayload)
    }

    /**
     * Kafka 수신 메시지를 처리하며, Host ID를 검증하여 본인 노드면 직접 중계하고 타 노드면 Redis Pub/Sub으로 릴레이합니다.
     */
    fun handleAgentResponse(event: AgentResponseEvent) {
        if (event.hostId == hostId) {
            // 1. 본인 노드인 경우: 직통 local SSE 소켓으로 배달
            logger.debug { "본인 노드 메시지 직통 배달 (hostId=$hostId): sessionId=${event.sessionId}" }
            dispatchToLocalClient(event)
        } else {
            // 2. 타 노드인 경우: Redis Pub/Sub을 통해 해당 노드로 유니캐스트 라우팅 (ADR 0001)
            logger.info { "타 노드 메시지 감지 ➔ Redis Pub/Sub 라우팅 (본인=$hostId, 타겟=${event.hostId}): sessionId=${event.sessionId}" }
            redisRoutingService.publishToTargetNode(event.hostId, event)
        }
    }

    /**
     * local SSE SendChannel 세션에 이벤트를 전송합니다.
     */
    fun dispatchToLocalClient(event: AgentResponseEvent) {
        val channel = sessionRegistry.getChannel(event.sessionId)
        if (channel != null) {
            val sseEvent = ServerSentEvent.builder<String>()
                .event(event.type)
                .data(objectMapper.writeValueAsString(event))
                .build()

            val result = channel.trySend(sseEvent)
            if (result.isSuccess) {
                logger.debug { "Client SSE 배달 성공: type=${event.type}, sessionId=${event.sessionId}" }
            } else {
                logger.warn { "Client SSE 배달 실패 (Channel full or closed): sessionId=${event.sessionId}" }
            }
        } else {
            logger.debug { "해당 sessionId의 로컬 세션을 찾을 수 없음: ${event.sessionId}" }
        }
    }
}
