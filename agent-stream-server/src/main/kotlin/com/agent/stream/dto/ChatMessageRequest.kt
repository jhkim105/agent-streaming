package com.agent.stream.dto

data class ChatMessageRequest(
    val sessionId: String,
    val conversationId: String = "", // 비즈니스 대화 식별자 (미전달 시 서버가 자동 생성/바인딩)
    val query: String
)

data class ChatMessageResponse(
    val status: String = "ACCEPTED",
    val message: String = "Research task queued successfully",
    val sessionId: String,
    val conversationId: String = ""
)

data class AgentActionRequest(
    val sessionId: String,
    val conversationId: String = "",
    val actionId: String,
    val payload: Map<String, Any> = emptyMap()
)
