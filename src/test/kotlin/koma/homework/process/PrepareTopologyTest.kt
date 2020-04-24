package koma.homework.process

import koma.homework.AppConfig
import org.apache.kafka.common.serialization.*
import org.apache.kafka.streams.StreamsBuilder
import org.junit.Before
import org.junit.Test
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.junit.After
import org.junit.Assert


class PrepareTopologyTest {

    private lateinit var testDriver: TopologyTestDriver
    private val factory = ConsumerRecordFactory("homework", IntegerSerializer(), StringSerializer())
    private val stringDeserializer = StringDeserializer()
    private val intDeserializer = IntegerDeserializer()
    
    @Before
    fun setUp() {
        val builder = StreamsBuilder()

        val source = builder.stream("homework", Consumed.with(Serdes.String(), Serdes.String()))
        val result = prepareTopology(source)
        result.to("homework-output")

        val topology = builder.build()
        testDriver = TopologyTestDriver(topology, AppConfig.getProps())
    }
    
    @After
    fun tearDown() {
        testDriver.close()
    }
    
    @Test
    fun `Confirmation event should be processed correctly`() {
        val input = "{\"after\":\"{\\\"_id\\\" : {\\\"\$oid\\\" : \\\"5cab7448afffbd5cf98cac46\\\"},\\\"taskId\\\" : \\\"TyazVPlL11HYaTGs1_dc1\\\",\\\"tGroupId\\\" : 1,\\\"levels\\\" : [1],\\\"createdAt\\\" : {\\\"\$numberLong\\\" : \\\"1554740296760\\\"},\\\"tUnits\\\" : [{\\\"tUnitId\\\" : \\\"TyazVPlL11HYaTGs1_dc1:1\\\",\\\"src\\\" : \\\"{2>The Great Charter of English liberty granted (under considerable duress) by King John at Runnymede on June 15, 1215<2}\\\",\\\"target\\\" : \\\"{2>The Great Charter of English liberty granted (under considerable duress) by King John at Runnymede on June 15, 1215<2}\\\",\\\"confirmedLevel\\\" : 1,\\\"locked\\\" : false,\\\"score\\\" : 0.0,\\\"grossScore\\\" : 0.0,\\\"roboTrans\\\" : \\\"\\\",\\\"roboScore\\\" : 0.0,\\\"transOriginDetail\\\" : \\\"null\\\",\\\"roboTransOriginDetail\\\" : \\\"nt_ai\\\",\\\"machineTrans\\\" : \\\"\\\",\\\"machineTransScore\\\" : 0.0,\\\"bestTmTrans\\\" : \\\"\\\",\\\"bestTMScore\\\" : 0.0,\\\"transOrigin\\\" : \\\"null\\\",\\\"createdAt\\\" : \\\"1554740266677\\\",\\\"createdBy\\\" : \\\"764\\\",\\\"modifiedAt\\\" : \\\"1554740295902\\\",\\\"modifiedBy\\\" : \\\"764\\\",\\\"qaDatas\\\" : [{\\\"level\\\" : 1,\\\"qaProcessed\\\" : false,\\\"qaIgnoredChecks\\\" : [\\\"Spellcheck:considerable\\\", \\\"Spellcheck:of\\\", \\\"Spellcheck:duress\\\", \\\"Spellcheck:English\\\", \\\"Spellcheck:granted\\\", \\\"Spellcheck:The\\\", \\\"Spellcheck:liberty\\\", \\\"Spellcheck:under\\\", \\\"Spellcheck:at\\\", \\\"Spellcheck:Runnymede\\\"]}],\\\"marksData\\\" : [{\\\"id\\\" : \\\"2\\\",\\\"content\\\" : \\\"<w:r><w:rPr><w:rFonts></w:rFonts><w:b w:val=\\\\\\\"false\\\\\\\"></w:b><w:i></w:i><w:caps w:val=\\\\\\\"false\\\\\\\"></w:caps><w:smallCaps w:val=\\\\\\\"false\\\\\\\"></w:smallCaps><w:strike w:val=\\\\\\\"false\\\\\\\"></w:strike><w:dstrike w:val=\\\\\\\"false\\\\\\\"></w:dstrike><w:color w:val=\\\\\\\"444333\\\\\\\"></w:color><w:spacing w:val=\\\\\\\"0\\\\\\\"></w:spacing><w:sz w:val=\\\\\\\"16\\\\\\\"></w:sz><w:u w:val=\\\\\\\"none\\\\\\\"></w:u><w:effect w:val=\\\\\\\"none\\\\\\\"></w:effect></w:rPr><w:t></w:t></w:r>\\\"}],\\\"targetMarksData\\\" : [{\\\"id\\\" : \\\"2\\\",\\\"content\\\" : \\\"<w:r><w:rPr><w:rFonts></w:rFonts><w:b w:val=\\\\\\\"false\\\\\\\"></w:b><w:i></w:i><w:caps w:val=\\\\\\\"false\\\\\\\"></w:caps><w:smallCaps w:val=\\\\\\\"false\\\\\\\"></w:smallCaps><w:strike w:val=\\\\\\\"false\\\\\\\"></w:strike><w:dstrike w:val=\\\\\\\"false\\\\\\\"></w:dstrike><w:color w:val=\\\\\\\"444333\\\\\\\"></w:color><w:spacing w:val=\\\\\\\"0\\\\\\\"></w:spacing><w:sz w:val=\\\\\\\"16\\\\\\\"></w:sz><w:u w:val=\\\\\\\"none\\\\\\\"></w:u><w:effect w:val=\\\\\\\"none\\\\\\\"></w:effect></w:rPr><w:t></w:t></w:r>\\\"}],\\\"levelEditingStats\\\" : {\\\"1\\\" : \\\"<m:editing-stats xmlns:m=\\\\\\\"http://www.memsource.com/mxlf/2.0\\\\\\\">\\\\r\\\\n<m:editing-time>129689</m:editing-time>\\\\r\\\\n<m:thinking-time>104033</m:thinking-time>\\\\r\\\\n</m:editing-stats>\\\"}}],\\\"paraId\\\" : \\\"0\\\",\\\"context\\\" : {\\\"filePart\\\" : \\\"word/document.xml::body\\\"},\\\"actionId\\\" : \\\"uo4pufh4le90\\\"}\",\"patch\":null,\"source\":{\"version\":\"0.8.1.Final\",\"name\":\"qa-debezium\",\"rs\":\"qa_set\",\"ns\":\"converter.tgroup\",\"sec\":1554740296,\"ord\":5,\"h\":1914709261723006611,\"initsync\":false},\"op\":\"c\",\"ts_ms\":1554740296776}\n"
        testDriver.pipeInput(factory.create(null as Int?, input))
        val result = outputToMap()
        Assert.assertEquals("Number of confirmed segments is not correct", 1, result["TyazVPlL11HYaTGs1_dc1"])
    }

    @Test
    fun `Confirmation and de-confirmation events should be processed correctly`() {
        val input1 = "{\"after\":\"{\\\"_id\\\" : {\\\"\$oid\\\" : \\\"5cab7448afffbd5cf98cac46\\\"},\\\"taskId\\\" : \\\"TyazVPlL11HYaTGs1_dc1\\\",\\\"tGroupId\\\" : 1,\\\"levels\\\" : [1],\\\"createdAt\\\" : {\\\"\$numberLong\\\" : \\\"1554740296760\\\"},\\\"tUnits\\\" : [{\\\"tUnitId\\\" : \\\"TyazVPlL11HYaTGs1_dc1:1\\\",\\\"src\\\" : \\\"{2>The Great Charter of English liberty granted (under considerable duress) by King John at Runnymede on June 15, 1215<2}\\\",\\\"target\\\" : \\\"{2>The Great Charter of English liberty granted (under considerable duress) by King John at Runnymede on June 15, 1215<2}\\\",\\\"confirmedLevel\\\" : 1,\\\"locked\\\" : false,\\\"score\\\" : 0.0,\\\"grossScore\\\" : 0.0,\\\"roboTrans\\\" : \\\"\\\",\\\"roboScore\\\" : 0.0,\\\"transOriginDetail\\\" : \\\"null\\\",\\\"roboTransOriginDetail\\\" : \\\"nt_ai\\\",\\\"machineTrans\\\" : \\\"\\\",\\\"machineTransScore\\\" : 0.0,\\\"bestTmTrans\\\" : \\\"\\\",\\\"bestTMScore\\\" : 0.0,\\\"transOrigin\\\" : \\\"null\\\",\\\"createdAt\\\" : \\\"1554740266677\\\",\\\"createdBy\\\" : \\\"764\\\",\\\"modifiedAt\\\" : \\\"1554740295902\\\",\\\"modifiedBy\\\" : \\\"764\\\",\\\"qaDatas\\\" : [{\\\"level\\\" : 1,\\\"qaProcessed\\\" : false,\\\"qaIgnoredChecks\\\" : [\\\"Spellcheck:considerable\\\", \\\"Spellcheck:of\\\", \\\"Spellcheck:duress\\\", \\\"Spellcheck:English\\\", \\\"Spellcheck:granted\\\", \\\"Spellcheck:The\\\", \\\"Spellcheck:liberty\\\", \\\"Spellcheck:under\\\", \\\"Spellcheck:at\\\", \\\"Spellcheck:Runnymede\\\"]}],\\\"marksData\\\" : [{\\\"id\\\" : \\\"2\\\",\\\"content\\\" : \\\"<w:r><w:rPr><w:rFonts></w:rFonts><w:b w:val=\\\\\\\"false\\\\\\\"></w:b><w:i></w:i><w:caps w:val=\\\\\\\"false\\\\\\\"></w:caps><w:smallCaps w:val=\\\\\\\"false\\\\\\\"></w:smallCaps><w:strike w:val=\\\\\\\"false\\\\\\\"></w:strike><w:dstrike w:val=\\\\\\\"false\\\\\\\"></w:dstrike><w:color w:val=\\\\\\\"444333\\\\\\\"></w:color><w:spacing w:val=\\\\\\\"0\\\\\\\"></w:spacing><w:sz w:val=\\\\\\\"16\\\\\\\"></w:sz><w:u w:val=\\\\\\\"none\\\\\\\"></w:u><w:effect w:val=\\\\\\\"none\\\\\\\"></w:effect></w:rPr><w:t></w:t></w:r>\\\"}],\\\"targetMarksData\\\" : [{\\\"id\\\" : \\\"2\\\",\\\"content\\\" : \\\"<w:r><w:rPr><w:rFonts></w:rFonts><w:b w:val=\\\\\\\"false\\\\\\\"></w:b><w:i></w:i><w:caps w:val=\\\\\\\"false\\\\\\\"></w:caps><w:smallCaps w:val=\\\\\\\"false\\\\\\\"></w:smallCaps><w:strike w:val=\\\\\\\"false\\\\\\\"></w:strike><w:dstrike w:val=\\\\\\\"false\\\\\\\"></w:dstrike><w:color w:val=\\\\\\\"444333\\\\\\\"></w:color><w:spacing w:val=\\\\\\\"0\\\\\\\"></w:spacing><w:sz w:val=\\\\\\\"16\\\\\\\"></w:sz><w:u w:val=\\\\\\\"none\\\\\\\"></w:u><w:effect w:val=\\\\\\\"none\\\\\\\"></w:effect></w:rPr><w:t></w:t></w:r>\\\"}],\\\"levelEditingStats\\\" : {\\\"1\\\" : \\\"<m:editing-stats xmlns:m=\\\\\\\"http://www.memsource.com/mxlf/2.0\\\\\\\">\\\\r\\\\n<m:editing-time>129689</m:editing-time>\\\\r\\\\n<m:thinking-time>104033</m:thinking-time>\\\\r\\\\n</m:editing-stats>\\\"}}],\\\"paraId\\\" : \\\"0\\\",\\\"context\\\" : {\\\"filePart\\\" : \\\"word/document.xml::body\\\"},\\\"actionId\\\" : \\\"uo4pufh4le90\\\"}\",\"patch\":null,\"source\":{\"version\":\"0.8.1.Final\",\"name\":\"qa-debezium\",\"rs\":\"qa_set\",\"ns\":\"converter.tgroup\",\"sec\":1554740296,\"ord\":5,\"h\":1914709261723006611,\"initsync\":false},\"op\":\"c\",\"ts_ms\":1554740296776}\n"
        testDriver.pipeInput(factory.create(null as Int?, input1))
        val input2 = "{\"after\":\"{\\\"_id\\\" : {\\\"\$oid\\\" : \\\"5cab7448afffbd5cf98cac46\\\"},\\\"taskId\\\" : \\\"TyazVPlL11HYaTGs1_dc1\\\",\\\"tGroupId\\\" : 1,\\\"levels\\\" : [1],\\\"createdAt\\\" : {\\\"\$numberLong\\\" : \\\"1554740296760\\\"},\\\"tUnits\\\" : [{\\\"tUnitId\\\" : \\\"TyazVPlL11HYaTGs1_dc1:1\\\",\\\"src\\\" : \\\"{2>The Great Charter of English liberty granted (under considerable duress) by King John at Runnymede on June 15, 1215<2}\\\",\\\"target\\\" : \\\"{2>The Great Charter of English liberty granted (under considerable duress) by King John at Runnymede on June 15, 1215<2}\\\",\\\"confirmedLevel\\\" : 0,\\\"locked\\\" : false,\\\"score\\\" : 0.0,\\\"grossScore\\\" : 0.0,\\\"roboTrans\\\" : \\\"\\\",\\\"roboScore\\\" : 0.0,\\\"transOriginDetail\\\" : \\\"null\\\",\\\"roboTransOriginDetail\\\" : \\\"nt_ai\\\",\\\"machineTrans\\\" : \\\"\\\",\\\"machineTransScore\\\" : 0.0,\\\"bestTmTrans\\\" : \\\"\\\",\\\"bestTMScore\\\" : 0.0,\\\"transOrigin\\\" : \\\"null\\\",\\\"createdAt\\\" : \\\"1554740266677\\\",\\\"createdBy\\\" : \\\"764\\\",\\\"modifiedAt\\\" : \\\"1554740295902\\\",\\\"modifiedBy\\\" : \\\"764\\\",\\\"qaDatas\\\" : [{\\\"level\\\" : 1,\\\"qaProcessed\\\" : false,\\\"qaIgnoredChecks\\\" : [\\\"Spellcheck:considerable\\\", \\\"Spellcheck:of\\\", \\\"Spellcheck:duress\\\", \\\"Spellcheck:English\\\", \\\"Spellcheck:granted\\\", \\\"Spellcheck:The\\\", \\\"Spellcheck:liberty\\\", \\\"Spellcheck:under\\\", \\\"Spellcheck:at\\\", \\\"Spellcheck:Runnymede\\\"]}],\\\"marksData\\\" : [{\\\"id\\\" : \\\"2\\\",\\\"content\\\" : \\\"<w:r><w:rPr><w:rFonts></w:rFonts><w:b w:val=\\\\\\\"false\\\\\\\"></w:b><w:i></w:i><w:caps w:val=\\\\\\\"false\\\\\\\"></w:caps><w:smallCaps w:val=\\\\\\\"false\\\\\\\"></w:smallCaps><w:strike w:val=\\\\\\\"false\\\\\\\"></w:strike><w:dstrike w:val=\\\\\\\"false\\\\\\\"></w:dstrike><w:color w:val=\\\\\\\"444333\\\\\\\"></w:color><w:spacing w:val=\\\\\\\"0\\\\\\\"></w:spacing><w:sz w:val=\\\\\\\"16\\\\\\\"></w:sz><w:u w:val=\\\\\\\"none\\\\\\\"></w:u><w:effect w:val=\\\\\\\"none\\\\\\\"></w:effect></w:rPr><w:t></w:t></w:r>\\\"}],\\\"targetMarksData\\\" : [{\\\"id\\\" : \\\"2\\\",\\\"content\\\" : \\\"<w:r><w:rPr><w:rFonts></w:rFonts><w:b w:val=\\\\\\\"false\\\\\\\"></w:b><w:i></w:i><w:caps w:val=\\\\\\\"false\\\\\\\"></w:caps><w:smallCaps w:val=\\\\\\\"false\\\\\\\"></w:smallCaps><w:strike w:val=\\\\\\\"false\\\\\\\"></w:strike><w:dstrike w:val=\\\\\\\"false\\\\\\\"></w:dstrike><w:color w:val=\\\\\\\"444333\\\\\\\"></w:color><w:spacing w:val=\\\\\\\"0\\\\\\\"></w:spacing><w:sz w:val=\\\\\\\"16\\\\\\\"></w:sz><w:u w:val=\\\\\\\"none\\\\\\\"></w:u><w:effect w:val=\\\\\\\"none\\\\\\\"></w:effect></w:rPr><w:t></w:t></w:r>\\\"}],\\\"levelEditingStats\\\" : {\\\"1\\\" : \\\"<m:editing-stats xmlns:m=\\\\\\\"http://www.memsource.com/mxlf/2.0\\\\\\\">\\\\r\\\\n<m:editing-time>129689</m:editing-time>\\\\r\\\\n<m:thinking-time>104033</m:thinking-time>\\\\r\\\\n</m:editing-stats>\\\"}}],\\\"paraId\\\" : \\\"0\\\",\\\"context\\\" : {\\\"filePart\\\" : \\\"word/document.xml::body\\\"},\\\"actionId\\\" : \\\"uo4pufh4le90\\\"}\",\"patch\":null,\"source\":{\"version\":\"0.8.1.Final\",\"name\":\"qa-debezium\",\"rs\":\"qa_set\",\"ns\":\"converter.tgroup\",\"sec\":1554740296,\"ord\":5,\"h\":1914709261723006611,\"initsync\":false},\"op\":\"c\",\"ts_ms\":1554740296776}\n"
        testDriver.pipeInput(factory.create(null as Int?, input2))
        val result = outputToMap()
        Assert.assertEquals("Number of confirmed segments is not correct", 0, result["TyazVPlL11HYaTGs1_dc1"])
    }

    @Test
    fun `Repeated confirmation event should increase the number of segments once`() {
        val input = "{\"after\":\"{\\\"_id\\\" : {\\\"\$oid\\\" : \\\"5cab7448afffbd5cf98cac46\\\"},\\\"taskId\\\" : \\\"TyazVPlL11HYaTGs1_dc1\\\",\\\"tGroupId\\\" : 1,\\\"levels\\\" : [1],\\\"createdAt\\\" : {\\\"\$numberLong\\\" : \\\"1554740296760\\\"},\\\"tUnits\\\" : [{\\\"tUnitId\\\" : \\\"TyazVPlL11HYaTGs1_dc1:1\\\",\\\"src\\\" : \\\"{2>The Great Charter of English liberty granted (under considerable duress) by King John at Runnymede on June 15, 1215<2}\\\",\\\"target\\\" : \\\"{2>The Great Charter of English liberty granted (under considerable duress) by King John at Runnymede on June 15, 1215<2}\\\",\\\"confirmedLevel\\\" : 1,\\\"locked\\\" : false,\\\"score\\\" : 0.0,\\\"grossScore\\\" : 0.0,\\\"roboTrans\\\" : \\\"\\\",\\\"roboScore\\\" : 0.0,\\\"transOriginDetail\\\" : \\\"null\\\",\\\"roboTransOriginDetail\\\" : \\\"nt_ai\\\",\\\"machineTrans\\\" : \\\"\\\",\\\"machineTransScore\\\" : 0.0,\\\"bestTmTrans\\\" : \\\"\\\",\\\"bestTMScore\\\" : 0.0,\\\"transOrigin\\\" : \\\"null\\\",\\\"createdAt\\\" : \\\"1554740266677\\\",\\\"createdBy\\\" : \\\"764\\\",\\\"modifiedAt\\\" : \\\"1554740295902\\\",\\\"modifiedBy\\\" : \\\"764\\\",\\\"qaDatas\\\" : [{\\\"level\\\" : 1,\\\"qaProcessed\\\" : false,\\\"qaIgnoredChecks\\\" : [\\\"Spellcheck:considerable\\\", \\\"Spellcheck:of\\\", \\\"Spellcheck:duress\\\", \\\"Spellcheck:English\\\", \\\"Spellcheck:granted\\\", \\\"Spellcheck:The\\\", \\\"Spellcheck:liberty\\\", \\\"Spellcheck:under\\\", \\\"Spellcheck:at\\\", \\\"Spellcheck:Runnymede\\\"]}],\\\"marksData\\\" : [{\\\"id\\\" : \\\"2\\\",\\\"content\\\" : \\\"<w:r><w:rPr><w:rFonts></w:rFonts><w:b w:val=\\\\\\\"false\\\\\\\"></w:b><w:i></w:i><w:caps w:val=\\\\\\\"false\\\\\\\"></w:caps><w:smallCaps w:val=\\\\\\\"false\\\\\\\"></w:smallCaps><w:strike w:val=\\\\\\\"false\\\\\\\"></w:strike><w:dstrike w:val=\\\\\\\"false\\\\\\\"></w:dstrike><w:color w:val=\\\\\\\"444333\\\\\\\"></w:color><w:spacing w:val=\\\\\\\"0\\\\\\\"></w:spacing><w:sz w:val=\\\\\\\"16\\\\\\\"></w:sz><w:u w:val=\\\\\\\"none\\\\\\\"></w:u><w:effect w:val=\\\\\\\"none\\\\\\\"></w:effect></w:rPr><w:t></w:t></w:r>\\\"}],\\\"targetMarksData\\\" : [{\\\"id\\\" : \\\"2\\\",\\\"content\\\" : \\\"<w:r><w:rPr><w:rFonts></w:rFonts><w:b w:val=\\\\\\\"false\\\\\\\"></w:b><w:i></w:i><w:caps w:val=\\\\\\\"false\\\\\\\"></w:caps><w:smallCaps w:val=\\\\\\\"false\\\\\\\"></w:smallCaps><w:strike w:val=\\\\\\\"false\\\\\\\"></w:strike><w:dstrike w:val=\\\\\\\"false\\\\\\\"></w:dstrike><w:color w:val=\\\\\\\"444333\\\\\\\"></w:color><w:spacing w:val=\\\\\\\"0\\\\\\\"></w:spacing><w:sz w:val=\\\\\\\"16\\\\\\\"></w:sz><w:u w:val=\\\\\\\"none\\\\\\\"></w:u><w:effect w:val=\\\\\\\"none\\\\\\\"></w:effect></w:rPr><w:t></w:t></w:r>\\\"}],\\\"levelEditingStats\\\" : {\\\"1\\\" : \\\"<m:editing-stats xmlns:m=\\\\\\\"http://www.memsource.com/mxlf/2.0\\\\\\\">\\\\r\\\\n<m:editing-time>129689</m:editing-time>\\\\r\\\\n<m:thinking-time>104033</m:thinking-time>\\\\r\\\\n</m:editing-stats>\\\"}}],\\\"paraId\\\" : \\\"0\\\",\\\"context\\\" : {\\\"filePart\\\" : \\\"word/document.xml::body\\\"},\\\"actionId\\\" : \\\"uo4pufh4le90\\\"}\",\"patch\":null,\"source\":{\"version\":\"0.8.1.Final\",\"name\":\"qa-debezium\",\"rs\":\"qa_set\",\"ns\":\"converter.tgroup\",\"sec\":1554740296,\"ord\":5,\"h\":1914709261723006611,\"initsync\":false},\"op\":\"c\",\"ts_ms\":1554740296776}\n"
        repeat(3) { testDriver.pipeInput(factory.create(null as Int?, input)) }
        val result = outputToMap()
        Assert.assertEquals("Number of confirmed segments is not correct", 1, result["TyazVPlL11HYaTGs1_dc1"])
    }


    @Test
    fun `Events from the sample input file should be processed correctly`() {
        pipeFileToDriver()
        val result = outputToMap()
        Assert.assertEquals("Number of confirmed segments is not correct", 0, result["jNazVPlL11HFhTGs1_dc1"])
        Assert.assertEquals("Number of confirmed segments is not correct", 2, result["TyazVPlL11HYaTGs1_dc1"])
    }

    @Test
    fun `Events from the sample input file should be processed correctly when piped repeatedly`() {
        repeat(3) { pipeFileToDriver() }
        val result = outputToMap()
        Assert.assertEquals("Number of confirmed segments is not correct", 0, result["jNazVPlL11HFhTGs1_dc1"])
        Assert.assertEquals("Number of confirmed segments is not correct", 2, result["TyazVPlL11HYaTGs1_dc1"])
    }

    private fun pipeFileToDriver() =
        PrepareTopologyTest::class.java.getResource("/kafka-messages.jsonline")
            .readText()
            .split("\n")
            .filter { it.isNotEmpty() }
            .forEach { testDriver.pipeInput(factory.create(null as Int?, it)) }

    private fun outputToMap(): Map<String, Int> {
        val result = mutableMapOf<String, Int>()
        do {
            val out = testDriver.readOutput("homework-output", stringDeserializer, intDeserializer)
            out?.let {
                result.put(out.key(), out.value())
            }
        } while (out != null)
        return result
    }
    
}