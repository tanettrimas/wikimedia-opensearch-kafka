package no.tanettrimas

import com.launchdarkly.eventsource.EventSource
import com.launchdarkly.eventsource.MessageEvent
import com.launchdarkly.eventsource.background.BackgroundEventHandler
import com.launchdarkly.eventsource.background.BackgroundEventSource
import com.launchdarkly.eventsource.background.ConnectionErrorHandler
import kotlinx.coroutines.*
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.net.URI
import kotlin.time.Duration.Companion.seconds

@OptIn(DelicateCoroutinesApi::class)
fun main(): Unit = runBlocking {
    val kafkaProperties = mapOf(
        CommonClientConfigs.CLIENT_ID_CONFIG to "wikimedia-v1",
        ProducerConfig.ACKS_CONFIG to "1",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:29092"
    )
    val topic = "wikimedia-changes-v1"

    val producer = KafkaProducer<String, String>(kafkaProperties)
    val builder = BackgroundEventSource.Builder(
        WikistreamProducer(kafkaProducer = producer, topic = topic),
        EventSource.Builder(URI("https://stream.wikimedia.org/v2/stream/recentchange"))
    )
    val eventSource = builder.build()

    val job = GlobalScope.launch {
        eventSource.use { eventSource ->
            while (isActive) {
                eventSource.start()
            }
            producer.close()
            println("Producer closed....")
        }
        println("Finished....")
    }
    Runtime.getRuntime().addShutdownHook(Thread {
        runBlocking {
            job.cancelAndJoin()
            println("Resources closed....")
        }
    })
    println("Let me delay...")
    delay(10.seconds)
    println("Finished delaying")
    job.cancelAndJoin()
    println("Finished with work, closing down")
}

class WikistreamProducer(private val kafkaProducer: KafkaProducer<String, String>, private val topic: String) :
    BackgroundEventHandler {
    /**
     * EventSource calls this method when the stream connection has been opened.
     * @throws Exception throwing an exception here will cause it to be logged and also sent to [.onError]
     */
    override fun onOpen() {
        println("EventSource opened")
    }

    /**
     * BackgroundEventSource calls this method when the stream connection has been closed.
     *
     *
     * This method is *not* called if the connection was closed due to a [ConnectionErrorHandler]
     * returning [ConnectionErrorHandler.Action.SHUTDOWN]; EventSource assumes that if you registered
     * such a handler and made it return that value, then you already know that the connection is being closed.
     *
     *
     * There is a known issue where `onClosed()` may or may not be called if the stream has been
     * permanently closed by calling `close()`.
     *
     * @throws Exception throwing an exception here will cause it to be logged and also sent to [.onError]
     */
    override fun onClosed() {
        kafkaProducer.close()
    }

    /**
     * EventSource calls this method when it has received a new event from the stream.
     * @param event the event name, from the `event:` line in the stream
     * @param messageEvent a [MessageEvent] object containing all the other event properties
     * @throws Exception throwing an exception here will cause it to be logged and also sent to [.onError]
     */
    override fun onMessage(event: String, messageEvent: MessageEvent) {
        println("Received message from eventsource")
        kafkaProducer.send(ProducerRecord(topic, messageEvent.data))
    }

    /**
     * EventSource calls this method when it has received a comment line from the stream (any line starting with a colon).
     * @param comment the comment line
     * @throws Exception throwing an exception here will cause it to be logged and also sent to [.onError]
     */
    override fun onComment(comment: String?) {
    }

    /**
     * This method will be called for all exceptions that occur on the socket connection (including
     * an [StreamHttpErrorException] if the server returns an unexpected HTTP status),
     * but only after the [ConnectionErrorHandler] (if any) has processed it.  If you need to
     * do anything that affects the state of the connection, use [ConnectionErrorHandler].
     *
     *
     * This method is *not* called if the error was already passed to a [ConnectionErrorHandler]
     * which returned [ConnectionErrorHandler.Action.SHUTDOWN]; EventSource assumes that if you registered
     * such a handler and made it return that value, then you do not want to handle the same error twice.
     *
     * @param t  a `Throwable` object
     */
    override fun onError(t: Throwable?) {
        println("Received error: ${t?.message}")
    }

}