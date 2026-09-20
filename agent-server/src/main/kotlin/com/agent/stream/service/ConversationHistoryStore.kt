package com.agent.stream.service

import com.agent.stream.dto.AgentEvent
import com.agent.stream.dto.ConversationDetailDto
import com.agent.stream.dto.ConversationRunDetailDto
import com.agent.stream.dto.ConversationSummaryDto
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.stereotype.Component
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

private val logger = KotlinLogging.logger {}

/**
 * 대화 스레드(Conversation) 및 멀티턴(Run) 스트리밍 이벤트를 영속성/인메모리에 축적 관리하는 저장소 서비스입니다.
 */
@Component
class ConversationHistoryStore {

    // conversationId -> ConversationSummaryDto 매핑
    private val conversations = ConcurrentHashMap<String, ConversationSummaryDto>()

    // conversationId -> 멀티턴(runId) 순서 보장 리스트 매핑
    private val conversationRunsMap = ConcurrentHashMap<String, CopyOnWriteArrayList<String>>()

    // runId -> 턴 메타데이터 및 상태 매핑
    private val runPromptMap = ConcurrentHashMap<String, String>()
    private val runTimelineMap = ConcurrentHashMap<String, CopyOnWriteArrayList<AgentEvent>>()
    private val runReportMap = ConcurrentHashMap<String, StringBuilder>()
    private val runA2uiMap = ConcurrentHashMap<String, String>()
    private val runCompletionMap = ConcurrentHashMap<String, Boolean>()
    private val runTimeMap = ConcurrentHashMap<String, Long>()

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
     * conversationId가 유효한지 확인하고 없으면 자동 생성하며, 턴 시작 시 질문 프롬프트를 등록합니다.
     */
    fun getOrCreateConversation(conversationIdInput: String?, query: String? = null, runId: String? = null): String {
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

        // 턴(runId) 등록
        if (!runId.isNullOrBlank()) {
            val runsList = conversationRunsMap.computeIfAbsent(conversationId) { CopyOnWriteArrayList() }
            if (!runsList.contains(runId)) {
                runsList.add(runId)
            }
            if (!query.isNullOrBlank()) {
                runPromptMap[runId] = query
            }
            runTimeMap[runId] = now
        }

        return conversationId
    }

    /**
     * 카프카/Redis로 전달받은 스트리밍 이벤트를 해당 턴(runId)에 축적하고, DONE 완결 시점에 상태를 확정합니다.
     */
    fun appendEvent(event: AgentEvent) {
        val conversationId = event.conversationId
        if (conversationId.isBlank()) return

        val runId = event.runId.ifBlank { "run-legacy" }

        // conversationRunsMap에 runId 연결 보장
        val runsList = conversationRunsMap.computeIfAbsent(conversationId) { CopyOnWriteArrayList() }
        if (!runsList.contains(runId)) {
            runsList.add(runId)
        }

        when (event.type) {
            "STATUS" -> {
                runTimelineMap.computeIfAbsent(runId) { CopyOnWriteArrayList() }.add(event)
            }
            "CHUNK" -> {
                runReportMap.computeIfAbsent(runId) { StringBuilder() }.append(event.content)
            }
            "A2UI_RENDER" -> {
                runA2uiMap[runId] = event.content
            }
            "DONE" -> {
                val now = System.currentTimeMillis()
                val smartTitle = (event.metadata["title"] as? String) ?: ""

                conversations[conversationId]?.let { existing ->
                    val finalTitle = if (smartTitle.isNotBlank()) smartTitle else existing.title

                    val finalCategory = if (event.content.contains("[TECH]") || event.content.contains("TECH")) "tech"
                    else if (event.content.contains("[BUSINESS]") || event.content.contains("BUSINESS")) "business"
                    else existing.category

                    logger.info { "대화 턴 완료 & 타이틀 갱신: conversationId=$conversationId, runId=$runId, title='$finalTitle'" }

                    conversations[conversationId] = existing.copy(
                        title = finalTitle,
                        category = finalCategory,
                        updatedAt = now
                    )
                }

                runCompletionMap[runId] = true
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
     * 특정 대화의 멀티턴 전체 히스토리(runs 목록)를 조회합니다.
     */
    fun getConversationDetail(conversationId: String): ConversationDetailDto? {
        val summary = conversations[conversationId] ?: return null
        val runIds = conversationRunsMap[conversationId] ?: emptyList()

        val runsList = runIds.map { runId ->
            ConversationRunDetailDto(
                runId = runId,
                userPrompt = runPromptMap[runId] ?: "",
                timelineEvents = runTimelineMap[runId] ?: emptyList(),
                fullReport = runReportMap[runId]?.toString() ?: "",
                a2uiPayload = runA2uiMap[runId],
                isCompleted = runCompletionMap[runId] ?: false,
                createdAt = runTimeMap[runId] ?: summary.createdAt
            )
        }

        val allTimeline = runsList.flatMap { it.timelineEvents }
        val fullCombinedReport = runsList.joinToString("\n\n---\n\n") { it.fullReport }.trim()
        val latestA2ui = runsList.lastOrNull { !it.a2uiPayload.isNullOrBlank() }?.a2uiPayload
        val allCompleted = runsList.all { it.isCompleted }

        return ConversationDetailDto(
            conversationId = summary.conversationId,
            title = summary.title,
            category = summary.category,
            createdAt = summary.createdAt,
            updatedAt = summary.updatedAt,
            runs = runsList,
            timelineEvents = allTimeline,
            fullReport = fullCombinedReport,
            a2uiPayload = latestA2ui,
            isCompleted = allCompleted
        )
    }
}
