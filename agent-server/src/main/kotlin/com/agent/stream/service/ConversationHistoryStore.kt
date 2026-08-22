package com.agent.stream.service

import com.agent.stream.dto.AgentEvent
import com.agent.stream.dto.ConversationDetailDto
import com.agent.stream.dto.ConversationSummaryDto
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.stereotype.Component
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

private val logger = KotlinLogging.logger {}

/**
 * 대화 스레드(Conversation) 및 개별 스트리밍 이벤트를 영속성/인메모리에 축적 관리하는 저장소 서비스입니다.
 */
@Component
class ConversationHistoryStore {

    // conversationId -> ConversationSummaryDto 매핑
    private val conversations = ConcurrentHashMap<String, ConversationSummaryDto>()

    // conversationId -> 타임라인 STATUS 이벤트 리스트 매핑
    private val timelineEventsMap = ConcurrentHashMap<String, CopyOnWriteArrayList<AgentEvent>>()

    // conversationId -> 축적된 마크다운 보고서 StringBuilder 매핑
    private val reportBuilderMap = ConcurrentHashMap<String, StringBuilder>()

    // conversationId -> A2UI 대시보드 JSON 저장 매핑
    private val a2uiPayloadMap = ConcurrentHashMap<String, String>()

    // conversationId -> DONE 완료 여부 매핑
    private val completionMap = ConcurrentHashMap<String, Boolean>()

    /**
     * 명시적으로 신규 대화 스레드를 생성합니다 (POST /api/conversations)
     */
    fun createConversation(): String {
        val conversationId = "conv-" + UUID.randomUUID().toString().take(8)
        val now = System.currentTimeMillis()
        val defaultTitle = "새 대화 (${conversationId.takeLast(4)})"

        val summary = ConversationSummaryDto(
            conversationId = conversationId,
            title = defaultTitle,
            category = "general",
            createdAt = now,
            updatedAt = now
        )
        conversations[conversationId] = summary
        logger.info { "신규 대화 스레드 생성 완료: conversationId=$conversationId" }
        return conversationId
    }

    /**
     * conversationId가 유효한지 확인하고 없으면 자동 생성합니다.
     */
    fun getOrCreateConversation(conversationIdInput: String?, query: String? = null): String {
        val conversationId = if (!conversationIdInput.isNullOrBlank()) {
            conversationIdInput
        } else {
            createConversation()
        }

        val now = System.currentTimeMillis()
        val formattedTitle = if (!query.isNullOrBlank()) {
            if (query.length > 35) query.take(35) + "..." else query
        } else {
            "새 대화 (${conversationId.takeLast(4)})"
        }

        conversations.compute(conversationId) { id, existing ->
            if (existing == null) {
                logger.info { "신규 대화 스레드 생성: conversationId=$id, title='$formattedTitle'" }
                ConversationSummaryDto(
                    conversationId = id,
                    title = formattedTitle,
                    category = "general",
                    createdAt = now,
                    updatedAt = now
                )
            } else {
                val updatedTitle = if (existing.title.startsWith("새 대화") && !query.isNullOrBlank()) {
                    formattedTitle
                } else existing.title

                existing.copy(title = updatedTitle, updatedAt = now)
            }
        }

        return conversationId
    }

    /**
     * 카프카/Redis로 전달받은 스트리밍 이벤트를 축적하고, DONE 스트림 완결 시점에 1회 일괄 저장 및 LLM 스마트 타이틀 갱신을 실행합니다.
     */
    fun appendEvent(event: AgentEvent) {
        val conversationId = event.conversationId
        if (conversationId.isBlank()) return

        when (event.type) {
            "STATUS" -> {
                timelineEventsMap.computeIfAbsent(conversationId) { CopyOnWriteArrayList() }.add(event)
            }
            "CHUNK" -> {
                reportBuilderMap.computeIfAbsent(conversationId) { StringBuilder() }.append(event.content)
            }
            "A2UI_RENDER" -> {
                a2uiPayloadMap[conversationId] = event.content
            }
            "DONE" -> {
                val now = System.currentTimeMillis()
                val smartTitle = (event.metadata["title"] as? String) ?: ""

                conversations[conversationId]?.let { existing ->
                    val finalTitle = if (smartTitle.isNotBlank()) smartTitle else existing.title

                    val finalCategory = if (event.content.contains("[TECH]") || event.content.contains("TECH")) "tech"
                    else if (event.content.contains("[BUSINESS]") || event.content.contains("BUSINESS")) "business"
                    else existing.category

                    logger.info { "대화 스트림 완결 1회 일괄 저장 & 스마트 타이틀 갱신 완료: conversationId=$conversationId, title='$finalTitle'" }

                    conversations[conversationId] = existing.copy(
                        title = finalTitle,
                        category = finalCategory,
                        updatedAt = now
                    )
                }

                completionMap[conversationId] = true
            }
        }
    }

    /**
     * 전체 대화 스레드 요약 목록을 최신 순으로 반환합니다. (GET /api/conversations)
     */
    fun getConversationSummaries(): List<ConversationSummaryDto> {
        return conversations.values.sortedByDescending { it.updatedAt }
    }

    /**
     * 특정 대화의 상세 타임라인 및 완결된 마크다운 보고서를 조회합니다.
     */
    fun getConversationDetail(conversationId: String): ConversationDetailDto? {
        val summary = conversations[conversationId] ?: return null
        val timeline = timelineEventsMap[conversationId] ?: emptyList()
        val fullReport = reportBuilderMap[conversationId]?.toString() ?: ""
        val a2uiPayload = a2uiPayloadMap[conversationId]
        val isCompleted = completionMap[conversationId] ?: false

        return ConversationDetailDto(
            conversationId = summary.conversationId,
            title = summary.title,
            category = summary.category,
            createdAt = summary.createdAt,
            updatedAt = summary.updatedAt,
            timelineEvents = timeline,
            fullReport = fullReport,
            a2uiPayload = a2uiPayload,
            isCompleted = isCompleted
        )
    }
}
