package com.agent.stream.session

import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import kotlinx.coroutines.channels.Channel
import org.springframework.http.codec.ServerSentEvent

class SessionRegistryTest : BehaviorSpec({

    given("SessionRegistry 인스턴스가 주어졌을 때") {
        val registry = SessionRegistry()

        `when`("새로운 SSE SendChannel 세션을 등록하면") {
            val sessionId = "test-session-123"
            val channel = Channel<ServerSentEvent<String>>()

            registry.register(sessionId, channel)

            then("세션이 정상 등록되고 조회가 가능해야 한다") {
                registry.hasSession(sessionId) shouldBe true
                registry.activeSessionCount() shouldBe 1
                registry.getChannel(sessionId) shouldNotBe null
            }

            `when`("해당 세션을 레지스트리에서 제거하면") {
                registry.remove(sessionId)

                then("세션이 말끔히 해제되어 활성 세션 수 0이 되어야 한다") {
                    registry.hasSession(sessionId) shouldBe false
                    registry.activeSessionCount() shouldBe 0
                    registry.getChannel(sessionId) shouldBe null
                }
            }
        }
    }
})
