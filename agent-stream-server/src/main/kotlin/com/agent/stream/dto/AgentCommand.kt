package com.agent.stream.dto

import java.util.UUID

/**
 * 클라이언트 ➔ 백엔드 ➔ Agent Worker로 전송되는 명령 도메인 모델입니다.
 */
data class AgentCommand(
    val commandId: String = UUID.randomUUID().toString(),
    val conversationId: String = "",
    val connectionId: String = "",
    val type: String = "RESEARCH", // RESEARCH, ACTION, CANCEL
    val payload: Map<String, Any> = emptyMap(),
    val timestamp: Long = System.currentTimeMillis()
)

data class AgentCommandResponse(
    val status: String = "ACCEPTED",
    val conversationId: String,
    val commandId: String,
    val message: String = "AgentCommand queued successfully"
)

data class CreateConversationResponse(
    val conversationId: String,
    val createdAt: Long = System.currentTimeMillis()
)
