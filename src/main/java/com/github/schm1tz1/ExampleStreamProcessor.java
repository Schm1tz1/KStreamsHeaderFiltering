package com.github.schm1tz1;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class ExampleStreamProcessor implements Processor<String, String, String, String> {
    private Boolean jsonFilterActionDrop;
    private ProcessorContext<String, String> context;
    private String jsonFilterField;
    private String jsonFilterValue;
    private Sensor sensorFiltered;
    private Sensor sensorIn;
    private Sensor sensorOut;


    /**
     *
     */
    public ExampleStreamProcessor()
    {

    }

    /**
     * @param context the context; may not be null
     */
    @Override
    public void init(ProcessorContext<String, String> context) {
        this.context = context;
        registerCustomMetrics();

        Processor.super.init(context);
    }

    private void registerCustomMetrics() {
        StreamsMetrics streamMetrics = context.metrics();

        sensorIn = streamMetrics.addRateTotalSensor("kstreams-template",
                context.applicationId(),
                "processor-in",
                Sensor.RecordingLevel.INFO,
                "task-id", "none");

        sensorOut = streamMetrics.addRateTotalSensor("kstreams-template",
                context.applicationId(),
                "processor-out",
                Sensor.RecordingLevel.INFO,
                "task-id", "none");
    }

    /**
     * @param record the record to process, will be forwarded/dropped if it matches the pattern depending on the configuration
     */
    @Override
    public void process(Record<String, String> record) {
        sensorIn.record();
        sensorOut.record();
    }

    /**
     *
     */
    @Override
    public void close() {
        Processor.super.close();
    }
}
