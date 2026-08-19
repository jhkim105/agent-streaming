package com.agent.stream.dto

import java.util.UUID

/**
 * Agent Worker ➔ 백엔드 ➔ 클라이언트 SSE로 전송되는 스트리밍 이벤트 도메인 모델입니다.
 */
data class AgentEvent(
    val eventId: String = UUID.randomUUID().toString(),
    val commandId: String = "",
    val conversationId: String = "",
    val hostId: String = "",
    val type: String = "STATUS", // INIT, STATUS, CHUNK, A2UI_RENDER, DONE, ERROR
    val content: String = "",
    val metadata: Map<String, Any> = emptyMap(),
    val timestamp: Long = System.currentTimeMillis()
)
