package com.agent.stream.service

import com.agent.stream.dto.AgentResponseEvent
import com.agent.stream.session.RedisSessionRegistry
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
    private val redisSessionRegistry: RedisSessionRegistry,
    private val redisStreamRoutingService: RedisStreamRoutingService,
    private val conversationHistoryStore: ConversationHistoryStore,
    private val hostId: String,
    private val objectMapper: ObjectMapper
) {
    @Value("\${app.kafka.topic-requests:agent-requests}")
    private lateinit var topicRequests: String

    /**
     * 질문을 conversationId 및 Session ID와 함께 카프카 요청 토픽으로 전송합니다. (hostId 미고정 - ADR 0003)
     */
    fun sendUserQuery(sessionId: String, conversationId: String, query: String) {
        // 대화 스레드 저장소에 질문 등록 및 초기화
        val validConvId = conversationHistoryStore.getOrCreateConversation(conversationId, query)

        val payload = mapOf(
            "sessionId" to sessionId,
            "conversationId" to validConvId,
            "query" to query
        )
        val jsonPayload = objectMapper.writeValueAsString(payload)

        logger.info { "Kafka 요청 토픽 전송 ($topicRequests - ADR 0003): conversationId=$validConvId, sessionId=$sessionId" }
        kafkaTemplate.send(topicRequests, sessionId, jsonPayload)
    }

    /**
     * 사용자의 A2UI 액션을 conversationId 및 Session ID와 함께 카프카 요청 토픽으로 전송합니다.
     */
    fun sendUserAction(sessionId: String, conversationId: String, actionId: String, payload: Map<String, Any>) {
        val validConvId = conversationHistoryStore.getOrCreateConversation(conversationId)

        val messagePayload = mapOf(
            "sessionId" to sessionId,
            "conversationId" to validConvId,
            "query" to "A2UI_ACTION:$actionId",
            "actionId" to actionId,
            "payload" to payload
        )
        val jsonPayload = objectMapper.writeValueAsString(messagePayload)

        logger.info { "Kafka A2UI Action 전송 ($topicRequests): conversationId=$validConvId, sessionId=$sessionId, actionId=$actionId" }
        kafkaTemplate.send(topicRequests, sessionId, jsonPayload)
    }

    /**
     * ADR 0003 컨슈머 시점 동적 세션 위치 조회:
     * Kafka 수신 메시지를 처리할 때 매 순간 Redis에서 해당 sessionId의 실재 소켓 노드(targetHostId)를 동적으로 조회하여 릴레이합니다.
     */
    fun handleAgentResponse(event: AgentResponseEvent) {
        // 1. 대화 이력 저장소에 실시간 이벤트 축적
        conversationHistoryStore.appendEvent(event)

        // 2. 컨슈머 시점 동적 세션 위치 조회 (RedisSessionRegistry)
        redisSessionRegistry.getSessionHost(event.sessionId)
            .defaultIfEmpty(event.hostId.ifBlank { hostId })
            .subscribe({ targetHostId ->
                if (targetHostId == hostId) {
                    // 본인 노드인 경우: 직통 local SSE 소켓으로 배달
                    logger.debug { "본인 노드 메시지 직통 배달 (hostId=$hostId): conversationId=${event.conversationId}, sessionId=${event.sessionId}" }
                    dispatchToLocalClient(event)
                } else {
                    // 타 노드인 경우: Redis Streams (stream:host:{targetHostId})를 통해 무유실 릴레이 (ADR 0003)
                    logger.info { "타 노드 메시지 감지 ➔ Redis Streams XADD 릴레이 (본인=$hostId, 타겟=$targetHostId): conversationId=${event.conversationId}, sessionId=${event.sessionId}" }
                    redisStreamRoutingService.publishToTargetStream(targetHostId, event.copy(hostId = targetHostId)).subscribe()
                }
            }, { err ->
                logger.error(err) { "세션 위치 동적 조회 중 오류 발생: sessionId=${event.sessionId}" }
                dispatchToLocalClient(event)
            })
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
            logger.debug { "해당 sessionId의 로컬 세션을 찾을 수 없음 (새로고침 또는 닫힘): sessionId=${event.sessionId}" }
        }
    }
}
