package koma.homework.process

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import koma.homework.model.ModEvent
import koma.homework.model.OpType
import koma.homework.model.TGroup
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.KStream


/**
 * Defines the topology of data transformation from a source stream to a result stream.
 */
fun prepareTopology(source: KStream<String, String>): KStream<String, Int> {

    val jsonMapper = ObjectMapper().apply {
        registerKotlinModule()
        configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    }

    return source
        .mapValues { modJson -> jsonMapper.readValue(modJson, ModEvent::class.java) }
        .filter { _, modEvent ->  modEvent.op == OpType.CREATE}
        .mapValues { modEvent -> jsonMapper.readValue(modEvent.after, TGroup::class.java) }
        .filter { _, tGroup -> !tGroup.levels.isNullOrEmpty() }
        .selectKey { _, tGroup -> tGroup.tUnits[0].tUnitId }
        .mapValues { tGroup -> if (tGroupConfirmed(tGroup)) 1 else 0 }
        .groupByKey() // group by segment ID, the last event's confirmation status is the relevant one for the given segment 
        .reduce { _, newValue -> newValue }
        .groupBy { tUnitId, confirmed -> KeyValue.pair(tUnitId.substringBefore(':'), confirmed) } // group by task ID and sum the statuses
        .reduce(
            { aggValue, newValue -> aggValue + newValue },
            { aggValue, oldValue -> aggValue - oldValue })
        .toStream()
}

private fun tGroupConfirmed(tGroup: TGroup) = tGroup.tUnits[0].confirmedLevel == tGroup.levels!![0]