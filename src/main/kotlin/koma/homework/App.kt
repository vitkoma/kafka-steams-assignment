package koma.homework

import koma.homework.process.prepareTopology
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Printed
import java.util.concurrent.CountDownLatch
import kotlin.system.exitProcess


/**
 * The application entry point. 
 * Based on the original skeleton.
 */
fun main() {
    
    val builder = StreamsBuilder()

    val source = builder.stream("homework", Consumed.with(Serdes.String(), Serdes.String()))
    source.print(Printed.toSysOut())
    // your code can start here

    val result = prepareTopology(source)
    result.print(Printed.toSysOut())
    result.to("homework-output")
    
    // end
    val topology = builder.build()
    System.out.println(topology.describe())
    val streams = KafkaStreams(topology, AppConfig.getProps())
    val latch = CountDownLatch(1)

    // attach shutdown handler to catch control-c
    Runtime.getRuntime().addShutdownHook(Thread {
        streams.close()
        latch.countDown()
    })

    try {
        streams.start()
        latch.await()
    } catch (e: Throwable) {
        exitProcess(1)
    }
    exitProcess(0)

}
