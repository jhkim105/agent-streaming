package com.agent.stream.dto

data class ChatMessageRequest(
    val sessionId: String,
    val query: String
)

data class ChatMessageResponse(
    val status: String = "ACCEPTED",
    val message: String = "Research task queued successfully",
    val sessionId: String
)
