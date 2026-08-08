package com.agent.stream.controller

import com.agent.stream.dto.*
import com.agent.stream.service.ConversationHistoryStore
import com.agent.stream.service.StreamService
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
    private val streamService: StreamService,
    private val conversationHistoryStore: ConversationHistoryStore,
    private val objectMapper: ObjectMapper
) {

    /**
     * 클라이언트와 SSE 단방향 스트림 연결을 수립합니다. (conversationId 전달 시 이전 대화에 바인딩)
     */
    @GetMapping("/stream", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun streamEvents(
        @RequestParam(required = false) conversationId: String? = null
    ): Flow<ServerSentEvent<String>> = callbackFlow {
        val sessionId = UUID.randomUUID().toString()
        val validConversationId = conversationHistoryStore.getOrCreateConversation(conversationId)

        logger.info { "새로운 SSE 연결 수립 요청: sessionId=$sessionId, conversationId=$validConversationId" }

        // 로컬 SessionRegistry에 스트리밍 소켓 채널 등록
        sessionRegistry.register(sessionId, this.channel)

        // INIT 이벤트 전달 (sessionId 및 conversationId 함께 반환)
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

        // Issue #2 세션 오삭제 방지: 현재 닫히는 채널 객체가 일치할 때만 안전하게 세션 제거
        awaitClose {
            logger.info { "SSE 연결 종료 감지 (Client disconnected): sessionId=$sessionId" }
            sessionRegistry.remove(sessionId, this.channel)
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
