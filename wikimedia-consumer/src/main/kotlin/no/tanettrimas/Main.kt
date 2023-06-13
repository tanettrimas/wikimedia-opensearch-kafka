package no.tanettrimas

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import kotlinx.coroutines.*
import org.apache.hc.core5.http.HttpHost
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.opensearch.client.opensearch.OpenSearchClient
import org.opensearch.client.opensearch.core.BulkRequest
import org.opensearch.client.opensearch.indices.CreateIndexRequest
import org.opensearch.client.transport.OpenSearchTransport
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder
import java.time.Duration

val jsonMapper = jacksonObjectMapper()
    .apply { this.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false) }

private const val OPENSEARCH_INDEX = "wikimedia"

fun main(): Unit = runBlocking {
    val consumerConfig = mapOf(
        ConsumerConfig.GROUP_ID_CONFIG to "wikimedia-consumer",
        ConsumerConfig.CLIENT_ID_CONFIG to "wikimedia-consumer",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false",
        ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG to "false",
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:29092"
    )
    val topic = "wikimedia-changes-v1"
    val kafkaConsumer = KafkaConsumer<String, String>(consumerConfig)
    val opensearch = createOpenSearchClient()

    val consumerTask = launch(Dispatchers.IO) {
        try {
            kafkaConsumer.use { consumer ->
                consumer.subscribe(listOf(topic))
                println("Wikimedia-consumer is subscribed to topic $topic")
                val indexRequest = CreateIndexRequest.Builder().index(OPENSEARCH_INDEX).build()
                val indexExists = opensearch.indices().exists { it.index(OPENSEARCH_INDEX) }.value()
                if (!indexExists) {
                    opensearch.indices().create(indexRequest)
                    println("Index created on opensearch")
                }

                while (isActive) {
                    val records = consumer.poll(Duration.ofSeconds(2))
                    println("Processed ${records.count()} messages")
                    val bulkRequestBuilder = BulkRequest
                        .Builder()
                        .index(OPENSEARCH_INDEX)

                    records.forEach { record ->
                        println("Received message in wikimedia-consumer: ${record.value()}")
                        val jsonNode: JsonNode = jsonMapper.readValue(record.value())
                        bulkRequestBuilder.operations { builder ->
                            builder.index { operationBuilder ->
                                operationBuilder.index(OPENSEARCH_INDEX)
                                operationBuilder.id(jsonNode.getId())
                                operationBuilder.document(jsonNode)
                            }
                        }
                        println("Added operation!")
                    }
                    if (records.count() > 0) {
                        val bulkRequest = bulkRequestBuilder.build()
                        opensearch.bulk(bulkRequest)
                        println("Sent a bulk request to opensearch with ${bulkRequest.operations().size} elements")
                        consumer.commitSync()
                    }
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

private fun createOpenSearchClient(): OpenSearchClient {
    val host = HttpHost("localhost", 9200)
    val transport: OpenSearchTransport = ApacheHttpClient5TransportBuilder
        .builder(host)
        .build()
    return OpenSearchClient(transport)
}

private fun JsonNode.getId() = this["meta"]["id"].asText()