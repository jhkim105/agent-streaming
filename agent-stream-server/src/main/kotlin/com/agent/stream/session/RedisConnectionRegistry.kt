package com.agent.stream.session

import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.data.redis.core.ReactiveStringRedisTemplate
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import java.time.Duration

private val logger = KotlinLogging.logger {}

/**
 * Redis 기반 분산 연결 레지스트리 서비스입니다.
 * - connection:host:{connectionId} ➔ hostId (소켓 보유 노드)
 * - command:connection:{commandId} ➔ connectionId (명령 발송 소켓)
 */
@Service
class RedisConnectionRegistry(
    private val redisTemplate: ReactiveStringRedisTemplate
) {
    private val connectionKeyPrefix = "connection:host:"
    private val commandKeyPrefix = "command:connection:"

    /**
     * SSE 연결 수립 시 해당 connectionId가 어느 hostId 노드에 맺어졌는지 Redis에 동적 등록합니다.
     */
    fun registerConnectionHost(connectionId: String, hostId: String): Mono<Boolean> {
        val key = "$connectionKeyPrefix$connectionId"
        logger.info { "Redis 연결 위치 등록: connectionId=$connectionId -> hostId=$hostId" }
        return redisTemplate.opsForValue()
            .set(key, hostId, Duration.ofHours(1))
    }

    /**
     * connectionId로 소켓이 위치한 타깃 서버 노드 ID(hostId)를 조회합니다.
     */
    fun getConnectionHost(connectionId: String): Mono<String> {
        val key = "$connectionKeyPrefix$connectionId"
        return redisTemplate.opsForValue().get(key)
    }

    /**
     * SSE 연결 종료 시 Redis 연결 위치 정보를 제거합니다.
     */
    fun removeConnectionHost(connectionId: String): Mono<Long> {
        val key = "$connectionKeyPrefix$connectionId"
        logger.info { "Redis 연결 위치 제거: connectionId=$connectionId" }
        return redisTemplate.delete(key)
    }

    /**
     * AgentCommand 수신 시 commandId와 해당 요청을 전송한 connectionId 매핑을 Redis에 등록합니다.
     */
    fun registerCommandConnection(commandId: String, connectionId: String): Mono<Boolean> {
        val key = "$commandKeyPrefix$commandId"
        logger.info { "Redis Command-Connection 매핑 등록: commandId=$commandId -> connectionId=$connectionId" }
        return redisTemplate.opsForValue()
            .set(key, connectionId, Duration.ofHours(1))
    }

    /**
     * commandId로 해당 AgentCommand를 보낸 connectionId를 조회합니다.
     */
    fun getConnectionByCommand(commandId: String): Mono<String> {
        val key = "$commandKeyPrefix$commandId"
        return redisTemplate.opsForValue().get(key)
    }
}
