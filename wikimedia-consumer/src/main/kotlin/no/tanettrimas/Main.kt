package no.tanettrimas

import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration

fun main(): Unit = runBlocking {
    val consumerConfig = mapOf(
        ConsumerConfig.GROUP_ID_CONFIG to "wikimedia-consumer",
        ConsumerConfig.CLIENT_ID_CONFIG to "wikimedia-consumer",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
        ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG to "false",
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:29092"
    )
    val topic = "wikimedia-changes-v1"
    val kafkaConsumer = KafkaConsumer<String, String>(consumerConfig)

    val consumerTask = launch(Dispatchers.IO) {
        try {
            kafkaConsumer.use { consumer ->
                consumer.subscribe(listOf(topic))
                println("Wikimedia-consumer is subscribed to topic $topic")
                while (isActive) {
                    val records = consumer.poll(Duration.ofSeconds(2))
                    records.forEach { record -> println("Received message in wikimedia-consumer: ${record.value()}") }
                    println("Processed ${records.count()} messages")
                }
            }


        } catch (e: WakeupException) {
            println("Consumer received wakeup, shutting down...")
        }
    }

    Runtime.getRuntime().addShutdownHook(Thread {
        runBlocking {
            kafkaConsumer.wakeup()
            consumerTask.cancelAndJoin()
        }
    })
}