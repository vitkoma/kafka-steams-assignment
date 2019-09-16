package koma.homework

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import java.util.*

/**
 * Application properties.
 */
object AppConfig {
    
    fun getProps() = Properties().apply { putAll(mapOf(
        
        StreamsConfig.APPLICATION_ID_CONFIG            to "homework-v0.2",
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG         to "localhost:9092",
        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG   to Serdes.String().javaClass,
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG to Serdes.Integer().javaClass,
        StreamsConfig.NUM_STREAM_THREADS_CONFIG        to 1,
        ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG     to 500
    
    ))}
}