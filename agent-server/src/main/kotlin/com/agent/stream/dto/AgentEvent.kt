package com.agent.stream.dto

import java.util.UUID

/**
 * Agent Worker ➔ Kafka ➔ 백엔드 ➔ 클라이언트 SSE로 전송되는 스트리밍 이벤트 도메인 모델입니다.
 *
 * @param sseEventId 단일 네트워크 패킷 식별자 (W3C SSE id 헤더 매핑)
 * @param runId 1개 턴(사용자 질문 + 에이전트 응답 세트) 식별자
 * @param messageId 에이전트 응답 내 특정 UI/텍스트 영역 식별자 (동일 messageId 시 In-place 갱신)
 * @param conversationId 대화 스레드 ID
 * @param hostId 게이트웨이 호스트 식별자
 * @param type INIT, STATUS, CHUNK, A2UI_RENDER, DONE, ERROR
 * @param content 이벤트 내용 (텍스트 또는 A2UI JSON 문자열)
 * @param metadata 추가 메타데이터 (스마트 타이틀 등)
 * @param timestamp 발생 시각
 */
data class AgentEvent(
    val sseEventId: String = "evt-" + UUID.randomUUID().toString().take(8),
    val runId: String = "",
    val messageId: String = "msg-default",
    val conversationId: String = "",
    val hostId: String = "",
    val type: String = "STATUS", // INIT, STATUS, CHUNK, A2UI_RENDER, DONE, ERROR
    val content: String = "",
    val metadata: Map<String, Any> = emptyMap(),
    val timestamp: Long = System.currentTimeMillis()
)
