package com.agent.stream.controller

import com.agent.stream.dto.ChatMessageRequest
import com.agent.stream.dto.ChatMessageResponse
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
    private val objectMapper: ObjectMapper
) {

    @GetMapping("/stream", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun streamEvents(): Flow<ServerSentEvent<String>> = callbackFlow {
        val sessionId = UUID.randomUUID().toString()
        logger.info { "새로운 SSE 연결 수립 요청: sessionId=$sessionId" }

        sessionRegistry.register(sessionId, this.channel)

        val initPayload = mapOf(
            "type" to "INIT",
            "sessionId" to sessionId,
            "content" to "SSE Connection Established"
        )
        val initEvent = ServerSentEvent.builder<String>()
            .event("INIT")
            .data(objectMapper.writeValueAsString(initPayload))
            .build()

        trySend(initEvent)

        awaitClose {
            logger.info { "SSE 연결 종료 감지 (Client disconnected): sessionId=$sessionId" }
            sessionRegistry.remove(sessionId)
        }
    }

    @PostMapping("/message")
    fun postMessage(@RequestBody request: ChatMessageRequest): ResponseEntity<ChatMessageResponse> {
        logger.info { "질문 제출 요청 수신: sessionId=${request.sessionId}, query='${request.query}'" }

        streamService.sendUserQuery(request.sessionId, request.query)

        val response = ChatMessageResponse(
            status = "ACCEPTED",
            message = "Research task queued successfully",
            sessionId = request.sessionId
        )
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response)
    }
}
