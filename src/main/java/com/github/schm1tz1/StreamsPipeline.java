package com.github.schm1tz1;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Kafka Streams Pipeline
 */
public class StreamsPipeline {
    final static Logger logger = LoggerFactory.getLogger(StreamsPipeline.class);
    final Properties streamsProperties;

    /**
     * Constructor to create App with properties
     *
     * @param streamsProperties Properties for the Kafka Streams application
     */
    public StreamsPipeline(Properties streamsProperties) {
        logger.debug("Starting EventFilterPipeline additional properties");
        this.streamsProperties = streamsProperties;
    }

    /**
     * Creates topology using Processor API
     *
     * @return topology object to be used with Kafka Streams
     */
    public Topology createStreamsTopology() {

        final StreamsBuilder builder = new StreamsBuilder();

        String inputTopicName = PipelineConfigTools.getPropertyChecked(streamsProperties, "streamsApp.inputTopic");
        String outputTopicName = PipelineConfigTools.getPropertyChecked(streamsProperties, "streamsApp.outputTopic");

        logger.info("Creating topology for " + inputTopicName + " -> " + outputTopicName);

        builder
                .stream(inputTopicName,
                        Consumed.with(Serdes.String(), Serdes.String()
                        )
                )
                .process(() -> new ExampleStreamProcessor())
                .to(outputTopicName, Produced.with(Serdes.String(), Serdes.String()));


        final Topology topology = builder.build();
        logger.debug(topology.describe().toString());

        return topology;
    }

    void run() {
        final Topology topology = createStreamsTopology();

        final KafkaStreams streams = new KafkaStreams(topology, streamsProperties);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
//            streams.cleanUp();
            streams.start();
            latch.await();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        System.exit(0);
    }

}
