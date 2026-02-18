package com.github.schm1tz1;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tools for Kafka Streams Pipeline configuration and initialization */
public class PipelineConfigTools {
  static final Logger logger = LoggerFactory.getLogger(PipelineConfigTools.class);

  /**
   * Configure streams application using defaults and a configuration file.
   *
   * @param configFiles Additional properties to read from input files
   * @return Kafka Streams configuration
   */
  public static Properties configureStreamsProperties(String... configFiles) {
    Properties properties = setDefaultStreamsProperties();

    for (String configFile : configFiles) {
      PipelineConfigTools.readPropertiesFile(properties, configFile);
    }

    return properties;
  }

  /**
   * Sets the default properties including bootstrap servers set to localhost. No pipeline
   * configuration included
   *
   * @return Properties object to start the pipeline
   */
  static Properties setDefaultStreamsProperties() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kstreams-header-filtering");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(
        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    properties.put(
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    logger.info("Default streams properties:" + properties);

    return properties;
  }

  /**
   * Fills properties object with configuration read from file input
   *
   * @param properties Properties object to modify
   * @param configFile configuration file to read
   */
  public static void readPropertiesFile(Properties properties, String configFile) {
    if (configFile != null) {
      logger.info("Reading properties file " + configFile);

      try (InputStream inputStream = new FileInputStream(configFile)) {
        Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);

        properties.load(reader);
        logger.info(
            properties.entrySet().stream()
                .map(e -> e.getKey() + " : " + e.getValue())
                .collect(Collectors.joining(", ")));
      } catch (FileNotFoundException e) {
        e.printStackTrace();
        logger.error("Input properties file " + configFile + " not found");
        System.exit(1);
      } catch (IOException e) {
        e.printStackTrace();
      }
    } else {
      logger.warn("No properties/config file specified, skipping!");
    }
  }

  /**
   * Adds basic monitoring interceptor configuration to streams properties. In case of TLS setups,
   * additional properties need to be configured, please also refer to examples and README
   *
   * @param streamProperties Properties object to modify
   */
  public static void addMonitoringInterceptorConfig(Properties streamProperties) {
    streamProperties.put(
        StreamsConfig.PRODUCER_PREFIX + ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
        "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");

    streamProperties.put(
        StreamsConfig.MAIN_CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
        "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor");
  }

  /**
   * Wrapper for properties getter to handle missing/null properties
   *
   * @param properties Properties object to use
   * @param key Key to retrieve property
   * @return Value of the property key
   */
  public static String getPropertyChecked(Properties properties, String key) {
    String value = properties.getProperty(key);
    if (value == null) {
      throw new RuntimeException("Mandatory property " + key + " is missing!");
    }
    return value;
  }

  /**
   * Returns the value of {@code key} from {@code properties}, or {@code defaultValue} if absent.
   *
   * @param properties the properties object to query
   * @param key the property key to look up
   * @param defaultValue value returned when the key is not present
   * @return the property value, or {@code defaultValue} if the key is absent
   */
  public static String getPropertyCheckedWithDefault(
      Properties properties, String key, String defaultValue) {
    String value = properties.getProperty(key);
    if (value == null) {
      return defaultValue;
    }
    return value;
  }
}
