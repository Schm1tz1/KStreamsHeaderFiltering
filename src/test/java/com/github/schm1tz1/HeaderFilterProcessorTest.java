package com.github.schm1tz1;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class HeaderFilterProcessorTest {

  private static Headers testHeaders;

  @BeforeAll
  static void createTestHeaders() {
    testHeaders =
        new RecordHeaders()
            .add("azure-event-hub", TestingTools.randomBytes(16))
            .add("x-datadog-sampling-priority", "42".getBytes(StandardCharsets.UTF_8))
            .add("x-datadog-trace-id", "666".getBytes(StandardCharsets.UTF_8))
            .add(
                "x-opt-enqueued-time",
                "Wed Feb 18 12:11:57 CET 2026".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void filterPredicatePassThrough() {
    HeaderFilterProcessor<String, String> stringHeaderFilterProcessor =
        new HeaderFilterProcessor<String, String>(header -> true, "Test-Processor");

    var result = stringHeaderFilterProcessor.filterHeaders(testHeaders);
    assertEquals(4, StreamSupport.stream(result.spliterator(), false).count());
  }

  @Test
  void filterRegExpPassThrough() {
    Pattern pattern = Pattern.compile(".*");
    HeaderFilterProcessor<String, String> stringHeaderFilterProcessor =
        new HeaderFilterProcessor<String, String>(
            HeaderFilterProcessor.matchingKey(pattern.asMatchPredicate()), "Test-Processor");
    var result = stringHeaderFilterProcessor.filterHeaders(testHeaders);
    assertEquals(4, StreamSupport.stream(result.spliterator(), false).count());
  }

  @Test
  void filterPredicateBlockAll() {
    HeaderFilterProcessor<String, String> stringHeaderFilterProcessor =
        new HeaderFilterProcessor<String, String>(header -> false, "Test-Processor");

    var result = stringHeaderFilterProcessor.filterHeaders(testHeaders);
    assertEquals(0, StreamSupport.stream(result.spliterator(), false).count());
  }

  @Test
  void filterRegExpBlockAll() {
    Pattern pattern = Pattern.compile("(!.*)¬");
    HeaderFilterProcessor<String, String> stringHeaderFilterProcessor =
        new HeaderFilterProcessor<String, String>(
            HeaderFilterProcessor.matchingKey(pattern.asMatchPredicate()), "Test-Processor");
    var result = stringHeaderFilterProcessor.filterHeaders(testHeaders);
    assertEquals(0, StreamSupport.stream(result.spliterator(), false).count());
  }

  @Test
  void filterPredicateNotDatadog() {
    HeaderFilterProcessor<String, String> stringHeaderFilterProcessor =
        new HeaderFilterProcessor<String, String>(
            header -> !header.key().matches("^x-datadog.*"), "Test-Processor");

    var result = stringHeaderFilterProcessor.filterHeaders(testHeaders);
    assertEquals(2, StreamSupport.stream(result.spliterator(), false).count());
  }

  @Test
  void filterRegExpNotDatadog() {
    Pattern pattern = Pattern.compile("^(?!x-datadog).*");
    HeaderFilterProcessor<String, String> stringHeaderFilterProcessor =
        new HeaderFilterProcessor<String, String>(
            HeaderFilterProcessor.matchingKey(pattern.asMatchPredicate()), "Test-Processor");
    var result = stringHeaderFilterProcessor.filterHeaders(testHeaders);
    assertEquals(2, StreamSupport.stream(result.spliterator(), false).count());
  }

  @Test
  void filterPredicateOnlyTime() {
    HeaderFilterProcessor<String, String> stringHeaderFilterProcessor =
        new HeaderFilterProcessor<String, String>(
            header -> header.key().matches(".*time$"), "Test-Processor");

    var result = stringHeaderFilterProcessor.filterHeaders(testHeaders);
    assertEquals(1, StreamSupport.stream(result.spliterator(), false).count());
  }

  @Test
  void filterRegExpOnlyTime() {
    Pattern pattern = Pattern.compile(".*time$");
    HeaderFilterProcessor<String, String> stringHeaderFilterProcessor =
        new HeaderFilterProcessor<String, String>(
            HeaderFilterProcessor.matchingKey(pattern.asMatchPredicate()), "Test-Processor");
    var result = stringHeaderFilterProcessor.filterHeaders(testHeaders);
    assertEquals(1, StreamSupport.stream(result.spliterator(), false).count());
  }

  @Test
  void filterRegExpMultipleNegations() {
    Pattern pattern = Pattern.compile("^(?!azure-.*|x-datadog-.*|x-opt-.*).*");
    HeaderFilterProcessor<String, String> stringHeaderFilterProcessor =
        new HeaderFilterProcessor<String, String>(
            HeaderFilterProcessor.matchingKey(pattern.asMatchPredicate()), "Test-Processor");
    var result = stringHeaderFilterProcessor.filterHeaders(testHeaders);
    assertEquals(0, StreamSupport.stream(result.spliterator(), false).count());
  }

  @Test
  void filterRegExpWithNegationFlag() {
    Pattern pattern = Pattern.compile("^(azure-.*|x-datadog-.*|x-opt-.*).*");
    HeaderFilterProcessor<String, String> stringHeaderFilterProcessor =
        new HeaderFilterProcessor<String, String>(
            HeaderFilterProcessor.matchingKey(pattern.asMatchPredicate()).negate(),
            "Test-Processor");
    var result = stringHeaderFilterProcessor.filterHeaders(testHeaders);
    assertEquals(0, StreamSupport.stream(result.spliterator(), false).count());
  }

  @Test
  void headersCount() {
    assertEquals(4, HeaderFilterProcessor.headersCount(testHeaders));
  }

  @Test
  void headersByteSize() {
    for (int i = 0; i < 1000; i++) {
      var sizeTestHeaders =
          new RecordHeaders()
              .add("header1", TestingTools.randomBytes(i))
              .add("header2", TestingTools.randomBytes(i + 1))
              .add("header3", null);
      assertEquals(
          2 * i + 1 + "header1".length() + "header2".length() + "header3".length(),
          HeaderFilterProcessor.headersByteSize(sizeTestHeaders));
    }
  }
}
