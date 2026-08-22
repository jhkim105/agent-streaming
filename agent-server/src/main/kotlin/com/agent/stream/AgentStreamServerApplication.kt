package com.agent.stream

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class AgentStreamServerApplication

fun main(args: Array<String>) {
    runApplication<AgentStreamServerApplication>(*args)
}
