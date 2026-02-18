package io.confluent.dbschenker.kafka.common;

import static io.confluent.dbschenker.kafka.model.WebArcDriverRecord.TYP_LENKEN;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.dbschenker.kafka.model.GfosAbsenceData;
import io.confluent.dbschenker.kafka.model.GfosBalanceData;
import io.confluent.dbschenker.kafka.model.GfosData;
import io.confluent.dbschenker.kafka.model.GfosTempWorkerData;
import io.confluent.dbschenker.kafka.model.PaisyData;
import io.confluent.dbschenker.kafka.model.WebArcDriverRecord;
import io.confluent.dbschenker.kafka.model.ZeusXData;
import io.confluent.kafka.schemaregistry.json.jackson.Jackson;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.test.TestRecord;
import org.testcontainers.containers.KafkaContainer;

public class TestingTools {

  private static final int HASH_LENGTH = 32;

  public static byte[] createRandomHash() {
    var hashRandom = new byte[HASH_LENGTH];
    try {
      SecureRandom.getInstanceStrong().nextBytes(hashRandom);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
    return hashRandom;
  }

  public static String createRandomKey() {
    long idRandom;
    try {
      idRandom = SecureRandom.getInstanceStrong().nextLong();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
    return String.valueOf(idRandom);
  }

  public static TestRecord<String, byte[]> createRandomTestRecord(Instant recordTimestamp) {

    var testRecord =
        new TestRecord<String, byte[]>(
            createRandomKey(),
            "b7FeNaZEn6zv+1nz5iwmxTFMjKCH627lpxus4nAcFH8vPQ==".getBytes(StandardCharsets.UTF_8),
            recordTimestamp);
    testRecord.headers().add(KafkaConfigTools.HASH_HEADER_KEY, createRandomHash());
    return testRecord;
  }

  public static <K, V> ProducerRecord producerRecordFromTestRecord(
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
    var testRecord = createRandomTestRecord(recordTimestamp);

    return new ProducerRecord<>(
        topic,
        null,
        testRecord.timestamp(),
        testRecord.getKey(),
        testRecord.getValue(),
        Arrays.asList(testRecord.getHeaders().toArray()));
  }

  public static Producer getKafkaTestProducer(
      String kafkaBootstrapServers, Serializer keySerializer, Serializer valueSerializer)
      throws IOException {
    var properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
    properties.put(ProducerConfig.ACKS_CONFIG, "all");

    return new KafkaProducer<>(properties, keySerializer, valueSerializer);
  }

  public static Producer<String, byte[]> getKafkaTestProducer(String kafkaBootstrapServers)
      throws IOException {
    var properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
    properties.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.StringSerializer.class);
    properties.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.ByteArraySerializer.class);
    properties.put(ProducerConfig.ACKS_CONFIG, "all");

    return new KafkaProducer<>(properties);
  }

  public static Producer<String, byte[]> getKafkaTestProducerE2EE(String kafkaBootstrapServers)
      throws IOException {
    var properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
    properties.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.StringSerializer.class);
    properties.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        io.confluent.encryption.serializers.json.SecuredKafkaJsonSerializer.class);
    properties.put(ProducerConfig.ACKS_CONFIG, "all");

    return new KafkaProducer<>(properties);
  }

  public static String getKafkaBootstrap(KafkaContainer kafka) {
    return kafka.getHost() + ":" + kafka.getMappedPort(9093);
  }

  public static void createTestTopic(KafkaContainer kafka, String topicName) throws Exception {
    var result =
        kafka.execInContainer(
            "sh",
            "-c",
            "kafka-topics --bootstrap-server localhost:9092 --create "
                + "--topic "
                + topicName
                + " "
                + "--partitions 6 "
                + "--replication-factor 1");
    if (result.getExitCode() != 0) {
      throw new RuntimeException("Topic creation failed");
    }
  }

  public static TestRecord<String, PaisyData> createPaisyRecordFromString(
      String inputJson, Instant recordTimestamp) throws IOException {
    var inputAsBytes = inputJson.getBytes(StandardCharsets.UTF_8);
    var objectMapper = Jackson.newObjectMapper();

    var paisyData = objectMapper.readValue(inputAsBytes, PaisyData.class);

    return new TestRecord<>(
        ProcessingTools.createPaisyHashedKey(paisyData), paisyData, recordTimestamp);
  }

  public static TestRecord<String, ZeusXData> createZeusXRecord(Instant recordTimestamp) {
    var paisyData = setPaisyFields(new PaisyData());

    return new TestRecord<>(
        ProcessingTools.createPaisyHashedKey(paisyData),
        convertPaisyToZeusX(paisyData),
        recordTimestamp);
  }

  public static TestRecord<String, GfosData> createGfosRecord(Instant recordTimestamp) {
    var paisyData = setPaisyFields(new PaisyData());

    return new TestRecord<>(
        ProcessingTools.createPaisyHashedKey(paisyData),
        convertPaisyToGfos(paisyData),
        recordTimestamp);
  }

  public static TestRecord<String, GfosData> createGfosRecordWithOrgId(
      Instant recordTimestamp, String newOrgId) {
    var paisyData = setPaisyFields(new PaisyData()).withORG_ID(newOrgId);

    return new TestRecord<>(
        ProcessingTools.createPaisyHashedKey(paisyData),
        convertPaisyToGfos(paisyData),
        recordTimestamp);
  }

  public static TestRecord<String, GfosData> createGfosRecordWithLambda(
      Instant recordTimestamp, Function<GfosData, GfosData> gfosDataFunction) {

    var paisyData = setPaisyFields(new PaisyData());
    var gfosData = convertPaisyToGfos(paisyData);

    if (Objects.nonNull(gfosDataFunction)) {
      gfosData = gfosDataFunction.apply(gfosData);
    }

    return new TestRecord<>(
        ProcessingTools.createPaisyHashedKey(paisyData), gfosData, recordTimestamp);
  }

  public static TestRecord<String, PaisyData> createPaisyRecordWithLambda(
      Instant recordTimestamp, Function<PaisyData, PaisyData> paisyDataFunction) {
    var paisyData = setPaisyFields(new PaisyData());
    if (Objects.nonNull(paisyDataFunction)) {
      paisyData = paisyDataFunction.apply(paisyData);
    }
    return new TestRecord<>(
        ProcessingTools.createPaisyHashedKey(paisyData), paisyData, recordTimestamp);
  }

  public static TestRecord<String, GfosAbsenceData> createGfosAbsenceRecordWithLambda(
      Instant recordTimestamp, Function<GfosAbsenceData, GfosAbsenceData> gfosDataFunction) {
    var gfosData = new GfosAbsenceData();
    if (Objects.nonNull(gfosDataFunction)) {
      gfosData = gfosDataFunction.apply(gfosData);
    }
    return new TestRecord<>(null, gfosData, recordTimestamp);
  }

  public static TestRecord<String, GfosBalanceData> createGfosBalanceRecordWithLambda(
      Instant recordTimestamp, Function<GfosBalanceData, GfosBalanceData> gfosDataFunction) {
    var gfosData = new GfosBalanceData();
    if (Objects.nonNull(gfosDataFunction)) {
      gfosData = gfosDataFunction.apply(gfosData);
    }
    return new TestRecord<>(null, gfosData, recordTimestamp);
  }

  public static TestRecord<String, GfosTempWorkerData> createGfosTempWorkerRecordWithLambda(
      Instant recordTimestamp,
      Function<GfosTempWorkerData, GfosTempWorkerData> workerDataFunction) {
    var gfosData = new GfosTempWorkerData();
    if (Objects.nonNull(workerDataFunction)) {
      gfosData = workerDataFunction.apply(gfosData);
    }
    return new TestRecord<>(null, gfosData, recordTimestamp);
  }

  public static TestRecord<String, WebArcDriverRecord> createWebArcRecordWithModifications(
      Instant recordTimestamp,
      Function<WebArcDriverRecord, WebArcDriverRecord> webArcModifications) {
    var webArcData = setWebArcData(new WebArcDriverRecord());
    if (Objects.nonNull(webArcModifications)) {
      webArcData = webArcModifications.apply(webArcData);
    }
    return new TestRecord<>(webArcData.KartenNr, webArcData, recordTimestamp);
  }

  private static WebArcDriverRecord setWebArcData(WebArcDriverRecord webArcDriverRecord) {
    return webArcDriverRecord
        .withMatchcode("42")
        .withKartenNr("XXXXXXXXXX")
        .withStruktureinheit("Frankfurt")
        .withZeitraum("14.03.2020")
        .withBeginn("4:26")
        .withEnde("5:26")
        .withTyp(TYP_LENKEN)
        .withZeiten("Ja");
  }

  public static TestRecord<String, GfosData> mapPaisyRecordToGfosRecord(
      TestRecord<String, PaisyData> paisyRecord) {
    var mappedValue = convertPaisyToGfos(paisyRecord.value());
    return new TestRecord<>(
        paisyRecord.key(), mappedValue, Instant.ofEpochMilli(paisyRecord.timestamp()));
  }

  private static ZeusXData convertPaisyToZeusX(PaisyData paisyData) {
    return new ZeusXData()
        .withPerson(paisyData.PERSONALNUMMER)
        .withPersonOrganizationalUnit("100705")
        .withPersonAdditionalInfoField1("Schenker Stuttgart Flughafen")
        .withPersonAdditionalInfoField3(ZeusXData.DEFAULT_PersonAdditionalInfoField3)
        .withPersonFamilyName(paisyData.NACHNAME)
        .withPersonGivenName(paisyData.VORNAME)
        .withPersonGroupAssignment1("1")
        .withPersonAccessBegin(
            ProcessingTools.mapDateFormatsPaisyToZeusX(paisyData.EINTRITTSDATUM));
    //
    // .withPersonAccessEnd(ProcessingTools.mapDateFormatsPaisyToZeusX(paisyData.AUSTRITTSDATUM));
  }

  private static GfosData convertPaisyToGfos(PaisyData paisyData) {
    return new GfosData()
        .withP_TYPE(GfosData.PTYPE_CHANGE)
        .withAKTDAT(ProcessingTools.mapDateFormatsPaisyToGfos(paisyData.KOSTENSTELLE_GUELTIGKEIT))
        .withSMAND("SDAG")
        .withEINTRITT(ProcessingTools.mapDateFormatsPaisyToGfos(paisyData.EINTRITTSDATUM))
        .withAUSTRITT(
            ProcessingTools.handleSpecialDateValueOrMapGfos(
                paisyData.AUSTRITTSDATUM, ProcessingTools::mapDateFormatsPaisyToGfos))
        .withSABRECHNUNGSKREIS(paisyData.ORG_ID)
        .withSWERK(paisyData.ORG_ID)
        .withKST_BAUART(paisyData.ORG_ABTEILUNG)
        .withSISTABT(paisyData.ORG_ABTEILUNG)
        .withSSTAMMKST(paisyData.KOSTENSTELLE)
        .withSISTKST(paisyData.KOSTENSTELLE)
        .withSPERS(paisyData.PERSONALNUMMER)
        .withSMITARBKZ(paisyData.PAISYFIRMA);
  }

  public static PaisyData setPaisyFields(PaisyData paisyData) {
    return paisyData
        .withPERSONALNUMMER_ERWEITERT("3785550200")
        .withPAISYFIRMA("550200")
        .withKALENDER_BUNDESLAND("61")
        .withTARIFGEBIET("BW01")
        .withMA_KATEGORIE("2")
        .withMA_SACHKONTOZUORDNUNG("1")
        .withSV_NUMMER("12010101A111")
        .withORG_ID("123")
        .withORG_ABTEILUNG("666")
        .withKOSTENSTELLE("999")
        .withPERSONALNUMMER("3785")
        .withVORNAME("Vorname1")
        .withNACHNAME("Zuname1")
        .withEINTRITTSDATUM("01.08.1985")
        .withERSTE_TAETIGKEITSSTAETTE_NR("010101")
        .withKOSTENSTELLE_GUELTIGKEIT("01.08.2023");
  }

  public static TestRecord<String, PaisyData> updatePaisyRecordJsonSr(
      TestRecord<String, PaisyData> newRecord) {
    var modifiedPaisyData =
        newRecord
            .value()
            .withSV_NUMMER(
                String.valueOf(ThreadLocalRandom.current().nextInt(111111111, 999999999)));
    return new TestRecord<String, PaisyData>(
        newRecord.key(), modifiedPaisyData, newRecord.getRecordTime().plus(1, ChronoUnit.MINUTES));
  }

  public static TestRecord<String, PaisyData> updatePaisyRecordValidityTimeRelevantChanges(
      TestRecord<String, PaisyData> newRecord) {
    var modifiedPaisyData = newRecord.value().withMA_SACHKONTOZUORDNUNG("3");
    return new TestRecord<>(
        newRecord.key(), modifiedPaisyData, newRecord.getRecordTime().plus(1, ChronoUnit.MINUTES));
  }

  public static Consumer<String, byte[]> getKafkaTestConsumer(String kafkaBootstrapServers)
      throws IOException {
    var properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
    properties.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.StringDeserializer.class);
    properties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.put(
        ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + TestingTools.class.getSimpleName());
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    return new KafkaConsumer<>(properties);
  }

  public static Consumer<String, byte[]> getKafkaTestConsumer(
      String kafkaBootstrapServers, Deserializer keyDeserializer, Deserializer valueDeserializer)
      throws IOException {
    var properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.put(
        ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + TestingTools.class.getSimpleName());
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    return new KafkaConsumer<>(properties, keyDeserializer, valueDeserializer);
  }

  public static Consumer<JsonNode, byte[]> getKafkaTestConsumerE2EE(String kafkaBootstrapServers)
      throws IOException {
    var properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
    properties.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.StringDeserializer.class);
    properties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        io.confluent.encryption.serializers.json.SecuredKafkaJsonSchemaDeserializer.class);
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.put(
        ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + TestingTools.class.getSimpleName());
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    return new KafkaConsumer<>(properties);
  }
}
