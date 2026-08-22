package com.agent.stream.controller

import com.agent.stream.dto.*
import com.agent.stream.service.ConversationHistoryStore
import com.agent.stream.service.RedisStreamRoutingService
import com.agent.stream.service.StreamService
import com.agent.stream.session.RedisConnectionRegistry
import com.agent.stream.session.SessionRegistry
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.http.codec.ServerSentEvent
import org.springframework.web.bind.annotation.*
import java.util.UUID

private val logger = KotlinLogging.logger {}

@RestController
@RequestMapping("/api/conversations")
class ConversationController(
    private val sessionRegistry: SessionRegistry,
    private val redisConnectionRegistry: RedisConnectionRegistry,
    private val redisStreamRoutingService: RedisStreamRoutingService,
    private val streamService: StreamService,
    private val conversationHistoryStore: ConversationHistoryStore,
    private val hostId: String,
    private val objectMapper: ObjectMapper
) {

    /**
     * 명시적으로 새로운 대화 스레드를 생성합니다. (POST /api/conversations)
     */
    @PostMapping
    fun createConversation(): ResponseEntity<CreateConversationResponse> {
        val conversationId = conversationHistoryStore.createConversation()
        logger.info { "새로운 대화 스레드 생성 API 요청 수신: conversationId=$conversationId" }
        return ResponseEntity.status(HttpStatus.CREATED).body(
            CreateConversationResponse(conversationId = conversationId)
        )
    }

    /**
     * 특정 대화 스레드에 대해 실시간 SSE 단방향 연결을 수립합니다. (GET /api/conversations/{conversationId}/events)
     * Issue #5 해결: trySend() 대신 코루틴 send()를 사용하여 배압(Backpressure)을 보장합니다.
     */
    @GetMapping("/{conversationId}/events", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun streamEvents(
        @PathVariable conversationId: String,
        @RequestHeader(name = "Last-Event-ID", required = false) lastEventId: String? = null
    ): Flow<ServerSentEvent<String>> = callbackFlow {
        val connectionId = "conn-" + UUID.randomUUID().toString()
        val validConversationId = conversationHistoryStore.getOrCreateConversation(conversationId)

        logger.info { "새로운 SSE 연결 수립 요청: connectionId=$connectionId, conversationId=$validConversationId, lastEventId=$lastEventId, hostId=$hostId" }

        // 1. 로컬 SessionRegistry에 connectionId 소켓 채널 등록
        sessionRegistry.register(connectionId, this.channel)

        // 2. Redis 연결 위치 동적 저장소에 소켓 위치 등록 (connection:host:{connectionId} -> hostId)
        redisConnectionRegistry.registerConnectionHost(connectionId, hostId).subscribe()

        // 3. INIT 이벤트 전달 (Issue #5: send()로 배압 지원)
        val initEventPayload = AgentEvent(
            eventId = "evt-init-" + UUID.randomUUID().toString().take(8),
            conversationId = validConversationId,
            hostId = hostId,
            type = "INIT",
            content = "SSE Connection Established",
            metadata = mapOf("connectionId" to connectionId)
        )
        val initSseEvent = ServerSentEvent.builder<String>()
            .id(initEventPayload.eventId)
            .event("INIT")
            .data(objectMapper.writeValueAsString(initEventPayload))
            .build()

        send(initSseEvent)

        // 4. W3C Last-Event-ID 커서 존재 시 끊어진 시점 이후의 미수신 이벤트를 Redis Stream에서 복원 릴레이
        if (lastEventId != null && lastEventId.isNotBlank()) {
            logger.info { "W3C Last-Event-ID 수신 ➔ 미수신 이벤트 복원 진행: lastEventId=$lastEventId" }
            redisStreamRoutingService.readStreamEventsAfter(lastEventId)
                .subscribe({ missedEvents ->
                    missedEvents.forEach { event ->
                        val replayEvent = ServerSentEvent.builder<String>()
                            .id(event.eventId)
                            .event(event.type)
                            .data(objectMapper.writeValueAsString(event))
                            .build()
                        try {
                            // Issue #5 배압 보장
                            this@callbackFlow.trySend(replayEvent)
                        } catch (e: Exception) {
                            logger.error(e) { "복원 이벤트 배달 에러" }
                        }
                    }
                }, { err ->
                    logger.error(err) { "Last-Event-ID 이벤트 복원 중 오류 발생: lastEventId=$lastEventId" }
                })
        }

        // 소켓 연결 해제 감지 시 Redis 연결 정보 안전 제거
        awaitClose {
            logger.info { "SSE 연결 종료 감지 (Client disconnected): connectionId=$connectionId" }
            sessionRegistry.remove(connectionId, this.channel)
            redisConnectionRegistry.removeConnectionHost(connectionId).subscribe()
        }
    }

    /**
     * 특정 대화 스레드에 AgentCommand(질문, A2UI 액션 등)를 제출합니다. (POST /api/conversations/{conversationId}/commands)
     */
    @PostMapping("/{conversationId}/commands")
    fun postCommand(
        @PathVariable conversationId: String,
        @RequestBody commandRequest: AgentCommand
    ): ResponseEntity<AgentCommandResponse> {
        val validConvId = conversationHistoryStore.getOrCreateConversation(
            conversationId,
            commandRequest.payload["query"] as? String
        )

        val fullCommand = commandRequest.copy(conversationId = validConvId)
        logger.info { "AgentCommand 수신: conversationId=$validConvId, connectionId=${fullCommand.connectionId}, type=${fullCommand.type}" }

        val assignedCommandId = streamService.submitCommand(fullCommand)

        val response = AgentCommandResponse(
            status = "ACCEPTED",
            conversationId = validConvId,
            commandId = assignedCommandId,
            message = "AgentCommand queued successfully"
        )
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response)
    }

    /**
     * 이전 대화 스레드 요약 목록을 조회합니다. (GET /api/conversations)
     */
    @GetMapping
    fun getConversations(): ResponseEntity<List<ConversationSummaryDto>> {
        val summaries = conversationHistoryStore.getConversationSummaries()
        return ResponseEntity.ok(summaries)
    }

    /**
     * 특정 대화 스레드의 상세 이력, 타임라인 및 완성된 리포트/A2UI를 조회합니다. (GET /api/conversations/{conversationId})
     */
    @GetMapping("/{conversationId}")
    fun getConversationDetail(
        @PathVariable conversationId: String
    ): ResponseEntity<ConversationDetailDto> {
        val detail = conversationHistoryStore.getConversationDetail(conversationId)
            ?: return ResponseEntity.notFound().build()
        return ResponseEntity.ok(detail)
    }
}
