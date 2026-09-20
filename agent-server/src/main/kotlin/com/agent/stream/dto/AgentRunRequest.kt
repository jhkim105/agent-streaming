package com.agent.stream.dto

import java.util.UUID

/**
 * 클라이언트 ➔ 백엔드 ➔ Agent Worker로 전송되는 1개 턴(Run) 실행 요청 모델입니다.
 */
data class AgentRunRequest(
    val runId: String = "run-" + UUID.randomUUID().toString().take(8),
    val conversationId: String = "",
    val connectionId: String = "",
    val type: String = "RESEARCH", // RESEARCH, ACTION, CANCEL
    val payload: Map<String, Any> = emptyMap(),
    val timestamp: Long = System.currentTimeMillis()
)

data class AgentRunResponse(
    val status: String = "ACCEPTED",
    val conversationId: String,
    val runId: String,
    val message: String = "Agent Run queued successfully"
)

data class CreateConversationResponse(
    val conversationId: String,
    val createdAt: Long = System.currentTimeMillis()
)
