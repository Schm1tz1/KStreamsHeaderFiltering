package com.github.schm1tz1;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.test.TestRecord;

public class TestingTools {

  private static final Random RANDOM = new Random();
  private static final SecureRandom SECURE_RANDOM;

  static {
    try {
      SECURE_RANDOM = SecureRandom.getInstanceStrong();
    } catch (NoSuchAlgorithmException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  static byte[] randomBytes(int size) {
    byte[] bytes = new byte[size];
    RANDOM.nextBytes(bytes);
    return bytes;
  }

  public static String createRandomKey() {
    return String.valueOf(SECURE_RANDOM.nextLong());
  }

  public static TestRecord<String, byte[]> createRandomTestRecord(Instant recordTimestamp) {
    return createRandomTestRecordWithRandomHeaders(recordTimestamp, List.of("hash-header-key"));
  }

  public static TestRecord<String, byte[]> createRandomTestRecordWithRandomHeaders(
      Instant recordTimestamp, List<String> headerKeys) {
    var testRecord =
        new TestRecord<String, byte[]>(createRandomKey(), randomBytes(4096), recordTimestamp);
    headerKeys.forEach(key -> testRecord.headers().add(key, randomBytes(16)));

    return testRecord;
  }

  public static <K, V> ProducerRecord<K, V> producerRecordFromTestRecord(
      String topic, TestRecord<K, V> testRecord) {
    return new ProducerRecord<>(
        topic,
        null,
        testRecord.timestamp(),
        testRecord.getKey(),
        testRecord.getValue(),
        testRecord.getHeaders());
  }

  public static ProducerRecord<String, byte[]> createRandomProducerRecord(
      String topic, Instant recordTimestamp) {
    return producerRecordFromTestRecord(topic, createRandomTestRecord(recordTimestamp));
  }

  private static Properties baseProducerProperties(String bootstrapServers) {
    var properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    return properties;
  }

  public static Producer getKafkaTestProducer(
      String kafkaBootstrapServers, Serializer keySerializer, Serializer valueSerializer)
      throws IOException {
    return new KafkaProducer<>(
        baseProducerProperties(kafkaBootstrapServers), keySerializer, valueSerializer);
  }

  public static Producer<String, byte[]> getKafkaTestProducer(String kafkaBootstrapServers)
      throws IOException {
    var properties = baseProducerProperties(kafkaBootstrapServers);
    properties.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.StringSerializer.class);
    properties.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.ByteArraySerializer.class);
    return new KafkaProducer<>(properties);
  }

  private static Properties baseConsumerProperties(String bootstrapServers) {
    var properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.put(
        ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + TestingTools.class.getSimpleName());
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return properties;
  }

  public static Consumer<String, byte[]> getKafkaTestConsumer(String kafkaBootstrapServers)
      throws IOException {
    var properties = baseConsumerProperties(kafkaBootstrapServers);
    properties.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.StringDeserializer.class);
    properties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
    return new KafkaConsumer<>(properties);
  }

  public static Consumer<String, byte[]> getKafkaTestConsumer(
      String kafkaBootstrapServers, Deserializer keyDeserializer, Deserializer valueDeserializer)
      throws IOException {
    return new KafkaConsumer<>(
        baseConsumerProperties(kafkaBootstrapServers), keyDeserializer, valueDeserializer);
  }

  public static boolean headerExists(Headers headers, String key) {
    return headers.headers(key).iterator().hasNext();
  }

  public static List<String> randomSubset(List<String> inputList, int size) {
    if (size > inputList.size()) {
      throw new IllegalArgumentException("Subset size " + size + " exceeds list size " + inputList.size());
    }
    List<String> shuffled = new ArrayList<>(inputList);
    Collections.shuffle(shuffled);
    return shuffled.subList(0, size);
  }}
