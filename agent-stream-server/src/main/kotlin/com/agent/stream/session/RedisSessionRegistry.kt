package com.agent.stream.session

import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.data.redis.core.ReactiveStringRedisTemplate
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import java.time.Duration

private val logger = KotlinLogging.logger {}

/**
 * L4/L7 로드밸런서 라운드로빈 환경 대응을 위한 Redis 기반 세션 소켓 동적 위치 저장소 서비스입니다.
 */
@Service
class RedisSessionRegistry(
    private val redisTemplate: ReactiveStringRedisTemplate
) {
    private val sessionKeyPrefix = "session:host:"

    /**
     * SSE 연결 수립 시 해당 sessionId가 어느 hostId 노드에 연결되었는지 Redis에 동적 등록합니다.
     */
    fun registerSessionHost(sessionId: String, hostId: String): Mono<Boolean> {
        val key = "$sessionKeyPrefix$sessionId"
        logger.info { "Redis 세션 위치 동적 등록: sessionId=$sessionId -> hostId=$hostId" }
        return redisTemplate.opsForValue()
            .set(key, hostId, Duration.ofHours(1))
    }

    /**
     * 카프카 응답 소비 시점(Consumer)에 매 순간 해당 sessionId의 실재 소켓 노드(targetHostId)를 동적으로 조회합니다.
     */
    fun getSessionHost(sessionId: String): Mono<String> {
        val key = "$sessionKeyPrefix$sessionId"
        return redisTemplate.opsForValue().get(key)
    }

    /**
     * SSE 연결 종료 시 Redis 세션 위치 정보를 제거합니다.
     */
    fun removeSessionHost(sessionId: String): Mono<Long> {
        val key = "$sessionKeyPrefix$sessionId"
        logger.info { "Redis 세션 위치 제거: sessionId=$sessionId" }
        return redisTemplate.delete(key)
    }
}
