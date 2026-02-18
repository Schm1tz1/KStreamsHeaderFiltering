package com.github.schm1tz1;

import org.apache.kafka.common.header.Header;
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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamsPipelineTest {

    static final Logger logger = LoggerFactory.getLogger(StreamsPipelineTest.class);
    private static final int MSG_COUNT = 10000;

    private static TestInputTopic<String, byte[]> inputTopic;
    private static TestOutputTopic<String, byte[]> outputTopic;
    private static TopologyTestDriver topologyTestDriver;

    final List<String> allHeaderKeys = List.of(
            "X-B3-ParentSpanId",
            "X-B3-Sampled",
            "X-B3-SpanId",
            "X-B3-TraceId",
            "baggage-microservice-name",
            "baggage_microservice-name",
            "X-ApplicationName",
            "X-CiamId",
            "X-FinOrVin",
            "X-ServiceName",
            "X-Timestamp",
            "X-TrackingId",
            "__TypeId__",
            "azure",
            "dd-pathway-ctx-base64",
            "properties",
            "system",
            "traceparent",
            "tracestate",
            "version",
            "x-datadog-parent-id",
            "x-datadog-sampling-priority",
            "x-datadog-tags",
            "x-datadog-trace-id",
            "x-opt-enqueued-time",
            "x-opt-offset",
            "x-opt-partition-key",
            "x-opt-sequence-number",
            "x-origin"
    );

    @BeforeEach
    void createTopologyTestDriver() {

        var streamsProperties = PipelineConfigTools.setDefaultStreamsProperties();
        streamsProperties.putAll(
                Map.ofEntries(
                        Map.entry("schema.registry.url", "mock://test"),
                        Map.entry("streamsApp.inputTopic", "inputTopic"),
                        Map.entry("streamsApp.outputTopic", "outputTopic"),
                        Map.entry("streamsApp.regExpString", "^(azure-.*|x-datadog-.*|x-opt-.*).*"),
                        Map.entry("streamsApp.negateRegExp", "true")));

        var eventFilterPipeline = new StreamsPipeline(streamsProperties);
        var topology = eventFilterPipeline.createStreamsTopology();

        topologyTestDriver = new TopologyTestDriver(topology, streamsProperties);

        inputTopic =
                topologyTestDriver.createInputTopic(
                        "inputTopic", Serdes.String().serializer(), Serdes.ByteArray().serializer());

        outputTopic =
                topologyTestDriver.createOutputTopic(
                        "outputTopic", Serdes.String().deserializer(), Serdes.ByteArray().deserializer());
    }

    @Test
    void singleRecordIsFiltered() {
        var testRecord =
                TestingTools.createRandomTestRecordWithRandomHeaders(
                        Instant.now(), List.of("hash-header-key", "azure-event-hub", "x-datadog-tracing"));

        inputTopic.pipeInput(testRecord);
        assertEquals(1, outputTopic.getQueueSize());

        var lastRecordHeaders = outputTopic.readRecord().headers();
        assertTrue(TestingTools.headerExists(lastRecordHeaders, "hash-header-key"));
        assertFalse(TestingTools.headerExists(lastRecordHeaders, "azure-event-hub"));
        assertFalse(TestingTools.headerExists(lastRecordHeaders, "x-datadog-tracing"));
    }

    @Test
    void massTestWithLargeHeaderSet() {
        for (int i = 0; i < MSG_COUNT; i++) {
            var headers = TestingTools.randomSubset(allHeaderKeys, 10);
            var testRecord =
                    TestingTools.createRandomTestRecordWithRandomHeaders(
                            Instant.now(), headers);

            inputTopic.pipeInput(testRecord);
        }
        assertEquals(MSG_COUNT, outputTopic.getQueueSize());

        var headersSet = outputTopic.readRecordsToList().stream().flatMap(
                        record -> StreamSupport.stream(record.headers().spliterator(), false))
                .map(Header::key)
                .collect(Collectors.toSet());

        assertFalse(headersSet.contains("azure-event-hub"));
        assertFalse(headersSet.contains("x-datadog-tracing"));
    }

    @AfterEach
    void cleanupTtd() {
        topologyTestDriver.close();
    }
}
