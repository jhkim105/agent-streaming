package com.agent.stream.dto

/**
 * AI 에이전트 및 스트리밍 서버 간에 오가는 카프카/Redis 응답 이벤트 DTO입니다.
 */
data class AgentResponseEvent(
    val sessionId: String,
    val conversationId: String = "", // 비즈니스 대화 식별자 (미전달 시 빈 문자열)
    val hostId: String,
    val type: String, // INIT | STATUS | CHUNK | A2UI_RENDER | DONE | ERROR
    val content: String,
    val metadata: EventMetadata = EventMetadata()
)

/**
 * 이벤트 부가 메타데이터입니다.
 */
data class EventMetadata(
    val step: String = "",
    val timestamp: Long = System.currentTimeMillis()
)
