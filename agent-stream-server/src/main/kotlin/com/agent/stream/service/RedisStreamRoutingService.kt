package com.agent.stream.service

import com.agent.stream.dto.AgentResponseEvent
import com.agent.stream.session.SessionRegistry
import com.fasterxml.jackson.databind.ObjectMapper
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import org.springframework.data.redis.connection.stream.ReadOffset
import org.springframework.data.redis.connection.stream.StreamOffset
import org.springframework.data.redis.core.ReactiveStringRedisTemplate
import org.springframework.http.codec.ServerSentEvent
import org.springframework.stereotype.Service
import reactor.core.Disposable
import reactor.core.publisher.Mono
import reactor.util.retry.Retry
import java.time.Duration

private val logger = KotlinLogging.logger {}

/**
 * ADR 0003: 노드별 레디스 스트림(stream:host:{hostId})을 활용한 무유실 라우팅 및 릴레이 서비스입니다.
 */
@Service
class RedisStreamRoutingService(
    private val redisTemplate: ReactiveStringRedisTemplate,
    private val sessionRegistry: SessionRegistry,
    private val hostId: String,
    private val objectMapper: ObjectMapper
) {
    private val streamKeyPrefix = "stream:host:"
    private var streamSubscription: Disposable? = null

    /**
     * 노드 기동 시 본인 이름의 레디스 스트림(stream:host:{localHostId}) 단 1개만 XREAD로 릴레이 대기합니다.
     */
    @PostConstruct
    fun initStreamListener() {
        val myStreamKey = "$streamKeyPrefix$hostId"
        logger.info { "노드전용 Redis Stream 리스너 기동 (ADR 0003): streamKey=$myStreamKey (Host ID=$hostId)" }

        val streamOffset = StreamOffset.create(myStreamKey, ReadOffset.latest())

        // 본인 노드의 Redis Stream을 XREAD 연속 수신
        streamSubscription = redisTemplate.opsForStream<String, String>()
            .read(streamOffset)
            .repeat()
            .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1)))
            .subscribe({ record ->
                try {
                    val eventJson = record.value["payload"]
                    if (!eventJson.isNullOrBlank()) {
                        val event = objectMapper.readValue(eventJson, AgentResponseEvent::class.java)
                        val streamRecordId = record.id.value

                        logger.debug { "노드전용 Stream 이벤트 수신 (ID=$streamRecordId): type=${event.type}, sessionId=${event.sessionId}" }
                        dispatchToLocalClient(event, streamRecordId)
                    }
                } catch (e: Exception) {
                    logger.error(e) { "Redis Stream 메시지 파싱 중 오류 발생: recordId=${record.id.value}" }
                }
            }, { err ->
                logger.error(err) { "Redis Stream 리스너 예외 발생" }
            })
    }

    @PreDestroy
    fun destroy() {
        streamSubscription?.dispose()
        logger.info { "노드전용 Redis Stream 리스너 종료: hostId=$hostId" }
    }

    /**
     * 타깃 노드의 전용 레디스 스트림(stream:host:{targetHostId})에 이벤트를 무유실 XADD 전송합니다.
     */
    open fun publishToTargetStream(targetHostId: String, event: AgentResponseEvent): Mono<String> {
        val targetStreamKey = "$streamKeyPrefix$targetHostId"
        val eventJson = objectMapper.writeValueAsString(event)
        val body = mapOf("payload" to eventJson, "sessionId" to event.sessionId)

        logger.info { "Redis Stream XADD 릴레이 (ADR 0003): targetStreamKey=$targetStreamKey, type=${event.type}, sessionId=${event.sessionId}" }
        return redisTemplate.opsForStream<String, String>()
            .add(targetStreamKey, body)
            .map { recordId -> recordId.value }
    }

    /**
     * W3C Last-Event-ID 커서 기반 Stream 복원: 특정 lastEventId 이후의 미열람 스트림 이벤트를 XREAD로 조회합니다.
     */
    fun readStreamEventsAfter(lastEventId: String): Mono<List<AgentResponseEvent>> {
        val myStreamKey = "$streamKeyPrefix$hostId"
        val readOffset = ReadOffset.from(lastEventId)
        val streamOffset = StreamOffset.create(myStreamKey, readOffset)

        return redisTemplate.opsForStream<String, String>()
            .read(streamOffset)
            .map { record ->
                val eventJson = record.value["payload"] ?: ""
                objectMapper.readValue(eventJson, AgentResponseEvent::class.java)
            }
            .collectList()
    }

    /**
     * local SSE SendChannel 세션에 이벤트를 전송하며, W3C Last-Event-ID(Stream Entry ID)를 헤더/데이터에 주입합니다.
     */
    private fun dispatchToLocalClient(event: AgentResponseEvent, streamRecordId: String) {
        val channel = sessionRegistry.getChannel(event.sessionId)
        if (channel != null) {
            val sseEvent = ServerSentEvent.builder<String>()
                .id(streamRecordId) // Redis Stream Entry ID를 SSE id로 전송 (W3C 표준)
                .event(event.type)
                .data(objectMapper.writeValueAsString(event))
                .build()

            val result = channel.trySend(sseEvent)
            if (result.isSuccess) {
                logger.debug { "Client SSE 배달 성공 (Stream ID=$streamRecordId): type=${event.type}, sessionId=${event.sessionId}" }
            } else {
                logger.warn { "Client SSE 배달 실패 (Channel full or closed): sessionId=${event.sessionId}" }
            }
        } else {
            logger.debug { "해당 sessionId의 로컬 세션을 찾을 수 없음 (새로고침 또는 닫힘): sessionId=${event.sessionId}" }
        }
    }
}
