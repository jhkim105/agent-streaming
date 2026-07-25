package com.agent.stream.service

import com.agent.stream.dto.AgentResponseEvent
import com.agent.stream.session.SessionRegistry
import com.fasterxml.jackson.databind.ObjectMapper
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.annotation.PostConstruct
import org.springframework.data.redis.core.ReactiveStringRedisTemplate
import org.springframework.data.redis.listener.ChannelTopic
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer
import org.springframework.http.codec.ServerSentEvent
import org.springframework.stereotype.Service

private val logger = KotlinLogging.logger {}

/**
 * 다중 노드 아키텍처(ADR 0001)에서 Host ID 기반으로 타겟 노드에 Redis Pub/Sub 유니캐스트를 전송하고 수신하는 라우팅 서비스입니다.
 */
@Service
class RedisRoutingService(
    private val redisTemplate: ReactiveStringRedisTemplate,
    private val listenerContainer: ReactiveRedisMessageListenerContainer,
    private val sessionRegistry: SessionRegistry,
    private val hostId: String,
    private val objectMapper: ObjectMapper
) {

    /**
     * 서버 기동 시 본인 Host ID 이름의 Redis 채널("host:{hostId}") 구독을 활성화합니다.
     */
    @PostConstruct
    fun initSubscribing() {
        val topicName = "host:$hostId"
        logger.info { "Redis Pub/Sub 구독 활성화: $topicName" }

        listenerContainer.receive(ChannelTopic(topicName))
            .subscribe { message ->
                try {
                    val rawJson = message.message
                    val event = objectMapper.readValue(rawJson, AgentResponseEvent::class.java)
                    logger.debug { "Redis Pub/Sub 메시지 수신: type=${event.type}, sessionId=${event.sessionId}" }

                    // 수신된 이벤트를 이 노드의 local SendChannel SSE 세션으로 중계
                    dispatchToLocalClient(event)
                } catch (e: Exception) {
                    logger.error(e) { "Redis Pub/Sub 수신 메시지 파싱 오류" }
                }
            }
    }

    /**
     * 타겟 노드의 Redis 채널로 이벤트를 Publish합니다.
     */
    fun publishToTargetNode(targetHostId: String, event: AgentResponseEvent) {
        val topicName = "host:$targetHostId"
        val jsonPayload = objectMapper.writeValueAsString(event)

        logger.info { "Redis Pub/Sub 발행 (타 노드 릴레이): targetHostId=$targetHostId, sessionId=${event.sessionId}" }
        redisTemplate.convertAndSend(topicName, jsonPayload).subscribe()
    }

    /**
     * local SSE SendChannel 세션에 이벤트를 직접 배달합니다.
     */
    private fun dispatchToLocalClient(event: AgentResponseEvent) {
        val channel = sessionRegistry.getChannel(event.sessionId)
        if (channel != null) {
            val sseEvent = ServerSentEvent.builder<String>()
                .event(event.type)
                .data(objectMapper.writeValueAsString(event))
                .build()

            val result = channel.trySend(sseEvent)
            if (result.isSuccess) {
                logger.debug { "Redis 릴레이 ➔ Client SSE 배달 성공: type=${event.type}, sessionId=${event.sessionId}" }
            } else {
                logger.warn { "Client SSE 배달 실패 (Channel full or closed): sessionId=${event.sessionId}" }
            }
        } else {
            logger.warn { "local 세션을 찾을 수 없음: sessionId=${event.sessionId}" }
        }
    }
}
