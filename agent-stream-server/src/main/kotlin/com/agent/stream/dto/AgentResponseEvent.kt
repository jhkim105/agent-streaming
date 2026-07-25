package com.agent.stream.dto

data class AgentResponseEvent(
    val sessionId: String,
    val hostId: String,
    val type: String, // INIT | STATUS | CHUNK | DONE | ERROR
    val content: String,
    val metadata: EventMetadata = EventMetadata()
)

data class EventMetadata(
    val step: String = "",
    val timestamp: Long = System.currentTimeMillis()
)
