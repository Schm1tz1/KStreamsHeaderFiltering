package com.github.schm1tz1;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StreamsPipelineTest {

    static final Logger logger = LoggerFactory.getLogger(StreamsPipelineTest.class);

    private static TestInputTopic<String, byte[]> inputTopic;
    private static TestOutputTopic<String, byte[]> outputTopic;
    private static TopologyTestDriver topologyTestDriver;

    @BeforeEach
    void createTopologyTestDriver() {


        var streamsProperties = PipelineConfigTools.setDefaultStreamsProperties();
        streamsProperties.putAll(
                Map.ofEntries(
                        Map.entry("schema.registry.url", "mock://test"),
                        Map.entry(
                                "streamsApp.inputTopic",
                                "inputTopic"),
                        Map.entry(
                                "streamsApp.outputTopic",
                                "outputTopic")));

        var eventFilterPipeline = new StreamsPipeline(streamsProperties);
        var topology = eventFilterPipeline.createStreamsTopology();

        topologyTestDriver = new TopologyTestDriver(topology, streamsProperties);

        inputTopic =
                topologyTestDriver.createInputTopic(
                        "inputTopic",
                        Serdes.String().serializer(),
                        Serdes.ByteArray().serializer());

        outputTopic =
                topologyTestDriver.createOutputTopic(
                        "outputTopic",
                        Serdes.String().deserializer(),
                        Serdes.ByteArray().deserializer());
    }

    @Test
    void nonMappableAbsencesAreSkipped() {
        inputTopic.pipeInput(TestingTools.createRandomTestRecord(Instant.now()));
        assertEquals(1, outputTopic.getQueueSize());
    }

    @AfterEach
    void cleanupTtd() {
        topologyTestDriver.close();
    }
}
