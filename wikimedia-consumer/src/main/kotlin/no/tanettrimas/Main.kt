package no.tanettrimas

import org.apache.kafka.clients.consumer.ConsumerConfig

fun main(): Unit {
    val consumerConfig = mapOf(
        ConsumerConfig.GROUP_ID_CONFIG to "wikimedia-consumer"
    )
}