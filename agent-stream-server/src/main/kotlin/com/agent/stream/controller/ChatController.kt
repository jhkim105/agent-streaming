package com.agent.stream.controller

import com.agent.stream.dto.*
import com.agent.stream.service.ConversationHistoryStore
import com.agent.stream.service.RedisStreamRoutingService
import com.agent.stream.service.StreamService
import com.agent.stream.session.RedisSessionRegistry
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
@RequestMapping("/api/chat")
class ChatController(
    private val sessionRegistry: SessionRegistry,
    private val redisSessionRegistry: RedisSessionRegistry,
    private val redisStreamRoutingService: RedisStreamRoutingService,
    private val streamService: StreamService,
    private val conversationHistoryStore: ConversationHistoryStore,
    private val hostId: String,
    private val objectMapper: ObjectMapper
) {

    /**
     * 클라이언트와 SSE 단방향 스트림 연결을 수립합니다. (ADR 0003: Redis 세션 위치 동적 등록 및 Last-Event-ID 수신)
     */
    @GetMapping("/stream", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun streamEvents(
        @RequestParam(required = false) conversationId: String? = null,
        @RequestHeader(name = "Last-Event-ID", required = false) lastEventId: String? = null
    ): Flow<ServerSentEvent<String>> = callbackFlow {
        val sessionId = UUID.randomUUID().toString()
        val validConversationId = conversationHistoryStore.getOrCreateConversation(conversationId)

        logger.info { "새로운 SSE 연결 수립 요청 (ADR 0003): sessionId=$sessionId, conversationId=$validConversationId, lastEventId=$lastEventId, hostId=$hostId" }

        // 1. 로컬 SessionRegistry에 스트리밍 소켓 채널 등록
        sessionRegistry.register(sessionId, this.channel)

        // 2. Redis 세션 위치 동적 저장소에 소켓 위치 등록 (session:host:{sessionId} -> localHostId)
        redisSessionRegistry.registerSessionHost(sessionId, hostId).subscribe()

        // 3. INIT 이벤트 전달 (sessionId 및 conversationId 함께 반환)
        val initPayload = mapOf(
            "type" to "INIT",
            "sessionId" to sessionId,
            "conversationId" to validConversationId,
            "content" to "SSE Connection Established"
        )
        val initEvent = ServerSentEvent.builder<String>()
            .event("INIT")
            .data(objectMapper.writeValueAsString(initPayload))
            .build()

        trySend(initEvent)

        // 4. W3C Last-Event-ID 커서 존재 시 끊어진 시점 이후의 미열람 토큰을 Redis Stream에서 복원 릴레이
        if (lastEventId != null && lastEventId.isNotBlank()) {
            logger.info { "W3C Last-Event-ID 수신 ➔ 미열람 스트림 복원 진행: lastEventId=$lastEventId" }
            redisStreamRoutingService.readStreamEventsAfter(lastEventId)
                .subscribe({ missedEvents ->
                    missedEvents.forEach { event ->
                        val replayEvent = ServerSentEvent.builder<String>()
                            .event(event.type)
                            .data(objectMapper.writeValueAsString(event))
                            .build()
                        trySend(replayEvent)
                    }
                }, { err ->
                    logger.error(err) { "Last-Event-ID 스트림 복원 중 오류 발생: lastEventId=$lastEventId" }
                })
        }

        // Issue #2 세션 오삭제 방지 & Redis 세션 위치 제거
        awaitClose {
            logger.info { "SSE 연결 종료 감지 (Client disconnected): sessionId=$sessionId" }
            sessionRegistry.remove(sessionId, this.channel)
            redisSessionRegistry.removeSessionHost(sessionId).subscribe()
        }
    }

    /**
     * 리서치 질문을 등록하고 백그라운드 AI 에이전트로 비동기 전송합니다.
     */
    @PostMapping("/message")
    fun postMessage(@RequestBody request: ChatMessageRequest): ResponseEntity<ChatMessageResponse> {
        val validConvId = conversationHistoryStore.getOrCreateConversation(request.conversationId, request.query)

        logger.info { "질문 제출 요청 수신: conversationId=$validConvId, sessionId=${request.sessionId}, query='${request.query}'" }

        streamService.sendUserQuery(request.sessionId, validConvId, request.query)

        val response = ChatMessageResponse(
            status = "ACCEPTED",
            message = "Research task queued successfully",
            sessionId = request.sessionId,
            conversationId = validConvId
        )
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response)
    }

    /**
     * 사용자의 A2UI 액션 버튼 선택을 전송합니다.
     */
    @PostMapping("/action")
    fun postAction(@RequestBody request: AgentActionRequest): ResponseEntity<ChatMessageResponse> {
        val validConvId = conversationHistoryStore.getOrCreateConversation(request.conversationId)

        logger.info { "A2UI 액션 수신: conversationId=$validConvId, sessionId=${request.sessionId}, actionId='${request.actionId}'" }

        streamService.sendUserAction(request.sessionId, validConvId, request.actionId, request.payload)

        val response = ChatMessageResponse(
            status = "ACCEPTED",
            message = "Action queued successfully",
            sessionId = request.sessionId,
            conversationId = validConvId
        )
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response)
    }

    /**
     * 이전 대화 스레드 요약 목록을 조회합니다. (사이드바 히스토리 목록용)
     */
    @GetMapping("/conversations")
    fun getConversations(): ResponseEntity<List<ConversationSummaryDto>> {
        val summaries = conversationHistoryStore.getConversationSummaries()
        return ResponseEntity.ok(summaries)
    }

    /**
     * 특정 대화 스레드의 상세 이력, 타임라인 및 완성된 리포트/A2UI를 조회합니다. (새로고침 복원 & 히스토리 상세용)
     */
    @GetMapping("/conversations/{conversationId}")
    fun getConversationDetail(
        @PathVariable conversationId: String
    ): ResponseEntity<ConversationDetailDto> {
        val detail = conversationHistoryStore.getConversationDetail(conversationId)
            ?: return ResponseEntity.notFound().build()
        return ResponseEntity.ok(detail)
    }
}
