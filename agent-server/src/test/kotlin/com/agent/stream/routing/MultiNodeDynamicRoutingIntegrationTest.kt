package com.agent.stream.routing

import com.agent.stream.dto.AgentEvent
import com.agent.stream.service.ConversationHistoryStore
import com.agent.stream.service.RedisStreamRoutingService
import com.agent.stream.service.StreamService
import com.agent.stream.session.RedisConnectionRegistry
import com.agent.stream.session.SessionRegistry
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import kotlinx.coroutines.channels.Channel
import org.mockito.BDDMockito.given
import org.mockito.Mockito.mock
import org.springframework.data.redis.core.ReactiveStringRedisTemplate
import org.springframework.http.codec.ServerSentEvent
import org.springframework.kafka.core.KafkaTemplate
import reactor.core.publisher.Mono

/**
 * Kotest BehaviorSpec BDD 스타일로 작성된 ADR 0003 노드별 Redis Streams 라우팅 및 컨슈머 동적 연결 위치 조회 통합 테스트입니다.
 */
class MultiNodeDynamicRoutingIntegrationTest : BehaviorSpec({

    val objectMapper: ObjectMapper = jacksonObjectMapper()

    given("ADR 0003: L4 라운드로빈 로드밸런서 환경에서 Node 1(소켓 보유)과 Node 2(명령 유입/이벤트 소비)가 존재할 때") {

        val node1HostId = "kotlin-node-1"
        val node1SessionRegistry = SessionRegistry()

        val node2HostId = "kotlin-node-2"
        val node2SessionRegistry = SessionRegistry()
        val conversationHistoryStore = ConversationHistoryStore()

        var lastPublishedTargetStreamKey = ""
        var lastPublishedPayloadJson = ""

        @Suppress("UNCHECKED_CAST")
        val fakeKafkaTemplate = mock(KafkaTemplate::class.java) as KafkaTemplate<String, String>
        val mockRedisTemplate = mock(ReactiveStringRedisTemplate::class.java)
        val mockRedisConnectionRegistry = mock(RedisConnectionRegistry::class.java)

        val node2RedisStreamRoutingService = object : RedisStreamRoutingService(
            redisTemplate = mockRedisTemplate,
            sessionRegistry = node2SessionRegistry,
            hostId = node2HostId,
            objectMapper = objectMapper
        ) {
            override fun publishToTargetStream(targetHostId: String, targetConnectionId: String, event: AgentEvent): Mono<String> {
                lastPublishedTargetStreamKey = "stream:host:$targetHostId"
                lastPublishedPayloadJson = objectMapper.writeValueAsString(event)

                if (targetHostId == node1HostId) {
                    val channel = node1SessionRegistry.getChannel(targetConnectionId)
                    if (channel != null) {
                        val sseEvent = ServerSentEvent.builder<String>()
                            .id(event.eventId)
                            .event(event.type)
                            .data(objectMapper.writeValueAsString(event))
                            .build()
                        channel.trySend(sseEvent)
                    }
                }
                return Mono.just("1786187000-0")
            }
        }

        val node2StreamService = StreamService(
            kafkaTemplate = fakeKafkaTemplate,
            sessionRegistry = node2SessionRegistry,
            redisConnectionRegistry = mockRedisConnectionRegistry,
            redisStreamRoutingService = node2RedisStreamRoutingService,
            conversationHistoryStore = conversationHistoryStore,
            hostId = node2HostId,
            objectMapper = objectMapper
        )

        `when`("Node 1에 사용자의 SSE 소켓(connectionId: conn-user-999)이 연결되어 있을 때") {
            val userConnectionId = "conn-user-999"
            val userCommandId = "cmd-user-111"
            val userSseChannel = Channel<ServerSentEvent<String>>(10)

            node1SessionRegistry.register(userConnectionId, userSseChannel)

            given(mockRedisConnectionRegistry.getConnectionByCommand(userCommandId))
                .willReturn(Mono.just(userConnectionId))
            given(mockRedisConnectionRegistry.getConnectionHost(userConnectionId))
                .willReturn(Mono.just(node1HostId))

            `when`("Node 2가 Kafka에서 해당 커맨드의 이벤트를 수신하면") {
                val incomingEvent = AgentEvent(
                    eventId = "evt-test-100",
                    commandId = userCommandId,
                    conversationId = "conv-test-999",
                    hostId = node2HostId,
                    type = "CHUNK",
                    content = "ADR 0003 Redis Streams 무유실 토큰 데이터",
                    metadata = mapOf("step" to "report_generation")
                )

                node2StreamService.handleAgentEvent(incomingEvent)

                then("Node 2는 Redis 동적 조회를 통해 소켓 위치가 Node 1임을 감지하고 Node 1의 스트림(stream:host:kotlin-node-1)으로 XADD 발행해야 한다") {
                    lastPublishedTargetStreamKey shouldBe "stream:host:kotlin-node-1"
                    lastPublishedPayloadJson shouldNotBe ""
                }

                then("Node 1의 사용자가 연결된 SSE 소켓으로 이벤트가 정상 배달되어야 한다") {
                    val receivedSse = userSseChannel.tryReceive().getOrNull()

                    receivedSse shouldNotBe null
                    receivedSse?.id() shouldBe "evt-test-100"
                    receivedSse?.event() shouldBe "CHUNK"
                    receivedSse?.data()!!.contains("ADR 0003 Redis Streams 무유실 토큰 데이터") shouldBe true
                }
            }
        }
    }
})
