package com.agent.stream.config

import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import org.springframework.data.redis.core.ReactiveStringRedisTemplate
import org.springframework.data.redis.listener.ChannelTopic
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer

private val logger = KotlinLogging.logger {}

@Configuration
class RedisConfig {

    @Bean
    fun reactiveStringRedisTemplate(connectionFactory: ReactiveRedisConnectionFactory): ReactiveStringRedisTemplate {
        return ReactiveStringRedisTemplate(connectionFactory)
    }

    /**
     * 게이트웨이 노드 자신의 Host ID에 해당하는 Redis Pub/Sub 채널("host:{hostId}")을 지속 구독하는 리액티브 리스너 컨테이너입니다.
     */
    @Bean
    fun reactiveRedisMessageListenerContainer(
        connectionFactory: ReactiveRedisConnectionFactory,
        hostId: String
    ): ReactiveRedisMessageListenerContainer {
        val container = ReactiveRedisMessageListenerContainer(connectionFactory)
        val channelTopic = ChannelTopic("host:$hostId")

        logger.info { "Redis Pub/Sub 채널 구독 등록 시작: ${channelTopic.topic}" }
        return container
    }
}
