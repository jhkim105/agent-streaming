package com.agent.stream.history

import com.agent.stream.dto.AgentResponseEvent
import com.agent.stream.dto.EventMetadata
import com.agent.stream.service.ConversationHistoryStore
import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe

/**
 * Kotest BehaviorSpec BDD 스타일로 작성된 DONE 완결 시점 1회 일괄 저장 및 LLM 스마트 타이틀 확정 검증 통합 테스트입니다.
 */
class ConversationHistoryIntegrationTest : BehaviorSpec({

    given("대화 이력 영속성 저장소(ConversationHistoryStore)가 준비되었을 때") {
        val historyStore = ConversationHistoryStore()

        `when`("사용자가 새로운 질문을 전송하여 conversationId가 생성되면") {
            val userQuery = "spring boot kotlin 조사해줘"
            val conversationId = historyStore.getOrCreateConversation(null, userQuery)

            then("신규 conversationId가 할당되고 질문 원본 텍스트로 1차 타이틀이 등록되어야 한다") {
                conversationId shouldNotBe ""
                conversationId.startsWith("conv-") shouldBe true

                val summaries = historyStore.getConversationSummaries()
                summaries shouldHaveSize 1
                summaries[0].conversationId shouldBe conversationId
                summaries[0].title shouldBe "spring boot kotlin 조사해줘"
            }

            `when`("스트리밍 도중 STATUS 및 CHUNK 이벤트가 발생할 때는 타이틀을 변경하지 않다가") {
                historyStore.appendEvent(
                    AgentResponseEvent(
                        sessionId = "sse-session-1",
                        conversationId = conversationId,
                        hostId = "kotlin-node-1",
                        type = "STATUS",
                        content = "🔍 질문 의도 분석 중...",
                        metadata = EventMetadata(step = "query_analysis")
                    )
                )

                historyStore.appendEvent(
                    AgentResponseEvent(
                        sessionId = "sse-session-1",
                        conversationId = conversationId,
                        hostId = "kotlin-node-1",
                        type = "CHUNK",
                        content = "# 📊 Spring Boot & Kotlin 리포트\n\n내용...",
                        metadata = EventMetadata(step = "report_generation")
                    )
                )

                `when`("최종 DONE 완결 이벤트가 스마트 타이틀(🌱 Spring Boot & Kotlin 동향)과 함께 수신되면") {
                    val finalSmartTitle = "🌱 Spring Boot & Kotlin 동향"

                    historyStore.appendEvent(
                        AgentResponseEvent(
                            sessionId = "sse-session-1",
                            conversationId = conversationId,
                            hostId = "kotlin-node-1",
                            type = "DONE",
                            content = "[TECH] 리포트 작성 완결",
                            metadata = EventMetadata(step = "completed", title = finalSmartTitle)
                        )
                    )

                    then("DONE 완료 시점에 단 1회 스마트 타이틀로 확정 갱신되고 1회 일괄 저장(Batch Save)되어야 한다") {
                        val summaries = historyStore.getConversationSummaries()
                        summaries[0].title shouldBe finalSmartTitle

                        val detail = historyStore.getConversationDetail(conversationId)
                        detail shouldNotBe null
                        detail!!.conversationId shouldBe conversationId
                        detail.title shouldBe finalSmartTitle
                        detail.fullReport shouldBe "# 📊 Spring Boot & Kotlin 리포트\n\n내용..."
                        detail.isCompleted shouldBe true
                    }
                }
            }
        }
    }
})
