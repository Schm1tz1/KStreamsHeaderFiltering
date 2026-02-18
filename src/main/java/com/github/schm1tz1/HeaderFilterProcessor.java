package com.github.schm1tz1;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Kafka Streams {@link Processor} that filters record headers based on a predicate.
 *
 * <p>Headers matching the predicate are retained in the output record; non-matching headers are
 * removed. The processor also records custom metrics tracking the number and byte size of input and
 * output headers.
 *
 * @param <K> the key type of the stream records
 * @param <V> the value type of the stream records
 */
public class HeaderFilterProcessor<K, V> implements Processor<K, V, K, V> {
  static final Logger logger = LoggerFactory.getLogger(HeaderFilterProcessor.class);
  private final Predicate<Header> predicate;
  private final String filterName;
  private ProcessorContext<K, V> context;
  private Sensor sensorInCount;
  private Sensor sensorInBytes;
  private Sensor sensorOutCount;
  private Sensor sensorOutBytes;
  private Sensor sensorIn;
  private Sensor sensorSkipped;
  private Sensor sensorOut;

  /**
   * Creates a processor that keeps headers whose keys match {@code filteringRegExp}.
   *
   * @param filteringRegExp regular expression applied to each header key
   * @param filterName human-readable name used in log and error messages
   */
  public HeaderFilterProcessor(String filteringRegExp, String filterName) {
    this(filteringRegExp, false, filterName);
  }

  /**
   * Creates a processor that retains or drops headers whose keys match {@code filteringRegExp}.
   *
   * @param filteringRegExp regular expression applied to each header key
   * @param negateRegExp when {@code true}, headers that match are dropped instead of kept
   * @param filterName human-readable name used in log and error messages
   */
  public HeaderFilterProcessor(String filteringRegExp, Boolean negateRegExp, String filterName) {
    Objects.requireNonNull(filteringRegExp);

    logger.info("Filtering record by regular expression: '" + filteringRegExp + "'");
    Pattern pattern = Pattern.compile(filteringRegExp);
    if (negateRegExp) {
      this.predicate = matchingKey(pattern.asMatchPredicate()).negate();
    } else {
      this.predicate = matchingKey(pattern.asMatchPredicate());
    }
    this.filterName = filterName;
  }

  /**
   * Creates a processor that keeps headers satisfying the given predicate.
   *
   * @param filteringCondition predicate applied to each header; matching headers are retained
   * @param filterName human-readable name used in log and error messages
   */
  public HeaderFilterProcessor(Predicate<Header> filteringCondition, String filterName) {
    Objects.requireNonNull(filteringCondition);

    logger.info("Filtering record by condition: " + filteringCondition);
    this.predicate = filteringCondition;
    this.filterName = filterName;
  }

  /**
   * Counts the number of headers in the given collection.
   *
   * @param headers the headers to count
   * @return total number of headers
   */
  static long headersCount(Headers headers) {
    long count = 0;
    for (Header ignored : headers) count++;
    return count;
  }

  /**
   * Computes the byte size of a single header (key + value).
   *
   * @param header the header to measure
   * @return combined byte length of the header key (UTF-8 encoded) and value; {@code null} parts
   *     contribute zero bytes
   */
  static long headerByteSize(Header header) {
    long size = 0;
    if (header.key() != null) size += header.key().getBytes(StandardCharsets.UTF_8).length;
    if (header.value() != null) size += header.value().length;
    return size;
  }

  /**
   * Computes the combined byte size of all headers in the collection.
   *
   * @param headers the headers to measure
   * @return total byte length of all header keys and values
   */
  static long headersByteSize(Headers headers) {
    long size = 0;
    for (Header h : headers) size += headerByteSize(h);
    return size;
  }

  /**
   * Wraps a key predicate into a header predicate that tests only the header key.
   *
   * @param keyPredicate predicate applied to the header key string
   * @return a header predicate that delegates to {@code keyPredicate} on the header key
   */
  static Predicate<Header> matchingKey(Predicate<String> keyPredicate) {
    return header -> keyPredicate.test(header.key());
  }

  /**
   * Initializes this processor with the given context and registers custom metrics.
   *
   * @param context the processor context; may not be null
   */
  @Override
  public void init(ProcessorContext<K, V> context) {
    this.context = context;
    registerCustomMetrics();
    Processor.super.init(context);
  }

  private void registerCustomMetrics() {
    StreamsMetrics streamMetrics = context.metrics();

    sensorIn =
        streamMetrics.addRateTotalSensor(
            "kstreams-header-filter",
            context.applicationId(),
            "processor-in",
            Sensor.RecordingLevel.INFO,
            "task-id",
            "none");

    sensorInCount =
        streamMetrics.addRateTotalSensor(
            "kstreams-header-filter",
            context.applicationId(),
            "input-header-count",
            Sensor.RecordingLevel.INFO,
            "task-id",
            "none");

    sensorInBytes =
        streamMetrics.addRateTotalSensor(
            "kstreams-header-filter",
            context.applicationId(),
            "input-header-bytes",
            Sensor.RecordingLevel.INFO,
            "task-id",
            "none");

    sensorOutCount =
        streamMetrics.addRateTotalSensor(
            "kstreams-header-filter",
            context.applicationId(),
            "output-header-count",
            Sensor.RecordingLevel.INFO,
            "task-id",
            "none");

    sensorOutBytes =
        streamMetrics.addRateTotalSensor(
            "kstreams-header-filter",
            context.applicationId(),
            "output-header-bytes",
            Sensor.RecordingLevel.INFO,
            "task-id",
            "none");

    sensorSkipped =
        streamMetrics.addRateTotalSensor(
            "kstreams-header-filter",
            context.applicationId(),
            "processor-skipped",
            Sensor.RecordingLevel.INFO,
            "task-id",
            "none");

    sensorOut =
        streamMetrics.addRateTotalSensor(
            "kstreams-header-filter",
            context.applicationId(),
            "processor-out",
            Sensor.RecordingLevel.INFO,
            "task-id",
            "none");
  }

  /**
   * Processes a single record by filtering its headers and forwarding the result downstream.
   *
   * <p>If an exception occurs during header processing, the original unmodified record is forwarded
   * and the skip metric is incremented.
   *
   * @param record the record whose headers should be filtered
   */
  @Override
  public void process(Record<K, V> record) {
    try {
      sensorIn.record();
      var filteredHeaders = processHeaders(record.headers());
      var filteredRecord = record.withHeaders(filteredHeaders);
      context.forward(filteredRecord);
      sensorOut.record();
    } catch (Exception e) {
      logger.error(
          filterName
              + ": skipping filtering of record ("
              + this.getRecordMetadata(record, context)
              + ") due to exception in processing:"
              + e);
      context.forward(record);
      sensorSkipped.record();
    }
  }

  /**
   * Filters the given headers using this processor's predicate and records metrics.
   *
   * @param headers the input headers to filter
   * @return a new {@link org.apache.kafka.common.header.Headers} instance containing only the
   *     headers that matched the predicate
   */
  Headers processHeaders(Headers headers) {
    long inCount = 0, inBytes = 0, outCount = 0, outBytes = 0;
    Headers result = new RecordHeaders();
    for (Header header : headers) {
      long size = headerByteSize(header);
      inCount++;
      inBytes += size;
      if (predicate.test(header)) {
        result.add(header);
        outCount++;
        outBytes += size;
      }
    }
    sensorInCount.record(inCount);
    sensorInBytes.record(inBytes);
    sensorOutCount.record(outCount);
    sensorOutBytes.record(outBytes);
    return result;
  }

  /**
   * Filters the given headers using this processor's predicate without recording metrics.
   *
   * @param headers the input headers to filter
   * @return a new {@link org.apache.kafka.common.header.Headers} instance containing only the
   *     headers that matched the predicate
   */
  Headers filterHeaders(Headers headers) {
    Headers result = new RecordHeaders();
    for (Header header : headers) {
      if (predicate.test(header)) {
        result.add(header);
      }
    }
    return result;
  }

  private String getRecordMetadata(Record papiRecord, ProcessorContext context) {
    var meta = context.recordMetadata().get();
    return "topic: "
        + meta.topic()
        + ", partition: "
        + meta.partition()
        + ", offset: "
        + meta.offset()
        + ", key: "
        + String.valueOf(papiRecord.key())
        + ", timestamp: "
        + papiRecord.timestamp();
  }

  /** Closes this processor and releases any resources. */
  @Override
  public void close() {
    Processor.super.close();
  }
}
