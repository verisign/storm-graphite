/*
 * Copyright 2014 VeriSign, Inc.
 *
 * VeriSign licenses this file to you under the Apache License, version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 */
package com.verisign.storm.metrics.reporters;

import com.verisign.ie.styx.avro.graphingMetrics.GraphingMetrics;
import com.verisign.storm.metrics.reporters.kafka.BaseKafkaReporter;
import com.verisign.storm.metrics.serializers.AvroRecordSerializer;
import io.confluent.kafka.schemaregistry.client.LocalSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.fest.assertions.api.Assertions.fail;

public class KafkaReporterTest {
  private static final int TEST_COUNT = 10;
  private static final Logger LOG = LoggerFactory.getLogger(KafkaReporterTest.class);

  private static final Integer ZOOKEEPER_PORT = 2181;
  private static final String ZOOKEEPER_HOST = "127.0.0.1";
  private static final String ZK_CONNECT = ZOOKEEPER_HOST + ":" + ZOOKEEPER_PORT;
  private static final String KAFKA_HOST = "127.0.0.1";
  private static final Integer KAFKA_PORT = 9092;
  private static final String KAFKA_BROKER_LIST = KAFKA_HOST + ":" + KAFKA_PORT;

  private String destinationTopic;
  private KafkaServerStartable kafkaServer;
  private TestingServer zookeeper;
  private BaseKafkaReporter kafkaReporter;
  private LocalSchemaRegistryClient schemaRegistryClient;

  private Properties getBrokerConfig() {
    Properties props = new Properties();
    props.put("broker.id", "0");
    props.put("host.name", KAFKA_HOST);
    props.put("port", KAFKA_PORT.toString());
    props.put("num.partitions", "1");
    props.put("auto.create.topics.enable", "true");
    props.put("message.max.bytes", "1000000");
    props.put("zookeeper.connect", ZK_CONNECT);

    return props;
  }

  @BeforeClass private void initializeCluster() {
    try {
      zookeeper = new TestingServer(ZOOKEEPER_PORT);
    }
    catch (Exception e) {
      LOG.error(e.getMessage());
    }

    KafkaConfig kafkaConfig = new KafkaConfig(getBrokerConfig());
    kafkaServer = new KafkaServerStartable(kafkaConfig);
    kafkaServer.startup();
  }

  private void initializeSchemaRegistryReporter() {
    destinationTopic = "schemaRegistryDestinationTopic";
    HashMap<String, Object> reporterConfig = new HashMap<String, Object>();
    reporterConfig.put(BaseKafkaReporter.KAFKA_BROKER_LIST_FIELD, KAFKA_BROKER_LIST);
    reporterConfig.put(BaseKafkaReporter.KAFKA_TOPIC_NAME_FIELD, destinationTopic);

    schemaRegistryClient = new LocalSchemaRegistryClient();
    try {
      schemaRegistryClient.register(destinationTopic + "-key", GraphingMetrics.getClassSchema());
    }
    catch (RestClientException e) {
      LOG.error("Failed to register schema: {}", GraphingMetrics.getClassSchema().toString(true));
    }
    catch (IOException e) {
      LOG.error("Failed to register schema: {}", GraphingMetrics.getClassSchema().toString(true));
    }

    final KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistryClient);

    kafkaReporter = new BaseKafkaReporter(reporterConfig) {
      @Override public KafkaProducer configureKafkaProducer(Properties producerProps) {
        return new KafkaProducer<Object, Object>(producerProps, serializer, serializer);
      }
    };


  }

  private void initializeAvroReporter() {
    destinationTopic = "avroDestinationTopic";
    HashMap<String, Object> config = new HashMap<String, Object>();
    config.put(BaseKafkaReporter.KAFKA_BROKER_LIST_FIELD, KAFKA_BROKER_LIST);
    config.put(BaseKafkaReporter.KAFKA_TOPIC_NAME_FIELD, destinationTopic);

    kafkaReporter = new BaseKafkaReporter(config) {
      @Override public KafkaProducer configureKafkaProducer(Properties producerProps) {
        AvroRecordSerializer serializer = new AvroRecordSerializer();
        return new KafkaProducer<GenericRecord, GenericRecord>(producerProps, serializer, serializer);
      }
    };
  }

  @DataProvider(name = "metrics") public Object[][] metricsProvider() {
    Random rng = new Random(System.currentTimeMillis());
    Object[][] testData = new Object[TEST_COUNT][];

    for (int i = 0; i < TEST_COUNT; i++) {
      List<Object> data = new ArrayList<Object>();

      String prefix = new BigInteger(100, rng).toString(32);
      data.add(prefix);

      String metricName = new BigInteger(50, rng).toString(32);
      data.add(metricName);

      Double metricValue = rng.nextDouble();
      data.add(metricValue);

      Double truncatedValue = Double.parseDouble(String.format("%2.2f", metricValue));
      data.add(truncatedValue);

      Long timestamp = System.currentTimeMillis();
      data.add(timestamp);

      testData[i] = data.toArray();
    }

    return testData;
  }

  @Test(dataProvider = "metrics")
  public void avroKafkaReporterTest(String metricPrefix, String metricKey, Double value, Double truncatedValue,
      long timestamp) {

    /* GIVEN: A Zookeeper instance, a Kafka broker, and a the Kafka adapter we're testing */
    initializeAvroReporter();
    SimpleConsumer kafkaConsumer = new SimpleConsumer(KAFKA_HOST, KAFKA_PORT, 10000, 1024000, "simpleConsumer");
    
    /* WHEN: A new metric is appended to the adapter's buffer and we tell the adapter to send its data */
    HashMap<String, Double> metrics = new HashMap<String, Double>();
    metrics.put(metricKey, value);

    kafkaReporter.appendToBuffer(metricPrefix, metrics, timestamp);
    try {
      kafkaReporter.sendBufferContents();

      // Allow the Kafka server time to commit into its log the message we sent it
      Thread.sleep(50);
    }
    catch (IOException e) {
      LOG.error(e.getMessage());
    }
    catch (InterruptedException e) {
    }

    /* WHEN: A Kafka consumer reads the latest message from the same topic on the Kafka server*/
    FetchRequest fetchRequest = new FetchRequestBuilder().addFetch(destinationTopic, 0, 0, 1000000).build();
    FetchResponse response = kafkaConsumer.fetch(fetchRequest);

    GenericRecord result = null;
    Iterator<MessageAndOffset> messageSetItr = response.messageSet(destinationTopic, 0).iterator();

    // Fast forward to the message at the latest offset in the topic
    MessageAndOffset latestMessage = null;
    while (messageSetItr.hasNext()) {
      latestMessage = messageSetItr.next();
    }

        /* WHEN: The latest message is decoded using the supplied Avro schema */
    ByteBuffer payload = latestMessage.message().payload();
    byte[] bytes = new byte[payload.limit()];
    payload.get(bytes);

    try {
      result = deserialize(bytes, GraphingMetrics.getClassSchema());
    }
    catch (IOException e) {
      fail("Failed to deserialize message:" + e.getMessage());
    }

    /* THEN: The field values of the decoded record should be the same as those of the input fields. */
    assertThat(result).isNotNull();
    assertThat(result.get("prefix")).isEqualTo(metricPrefix);
    assertThat(result.get("reportTime")).isEqualTo(timestamp);
    assertThat(((Map) result.get("metricValues")).get(metricKey)).isEqualTo(truncatedValue);
  }

  @Test(dataProvider = "metrics")
  public void schemaRegistryKafkaReporterTest(String metricPrefix, String metricKey, Double value,
      Double truncatedValue,
      long timestamp) {

    initializeSchemaRegistryReporter();
    SimpleConsumer kafkaConsumer = new SimpleConsumer(KAFKA_HOST, KAFKA_PORT, 10000, 1024000, "simpleConsumer");
    KafkaAvroDecoder decoder = new KafkaAvroDecoder(schemaRegistryClient);

    HashMap<String, Double> metrics = new HashMap<String, Double>();
    metrics.put(metricKey, value);

    kafkaReporter.appendToBuffer(metricPrefix, metrics, timestamp);
    try {
      kafkaReporter.sendBufferContents();

      // Allow the Kafka server time to commit into its log the message we sent it
      Thread.sleep(50);
    }
    catch (IOException e) {
      LOG.error(e.getMessage());
    }
    catch (InterruptedException e) {
    }

    FetchRequest fetchRequest = new FetchRequestBuilder().addFetch(destinationTopic, 0, 0, 1000000).build();
    FetchResponse response = kafkaConsumer.fetch(fetchRequest);

    GenericRecord result = null;
    Iterator<MessageAndOffset> messageSetItr = response.messageSet(destinationTopic, 0).iterator();

    // Fast forward to the message at the latest offset in the topic
    MessageAndOffset latestMessage = null;
    while (messageSetItr.hasNext()) {
      latestMessage = messageSetItr.next();
    }

    /* WHEN: The latest message is decoded using the supplied Avro schema */
    ByteBuffer payload = latestMessage.message().payload();
    byte[] bytes = new byte[payload.limit()];
    payload.get(bytes);

    try {
      result = (GenericRecord) decoder.fromBytes(bytes);
    }
    catch (SerializationException e) {
      fail("Failed to deserialize message:" + e.getMessage());
    }

    assertThat(result).isNotNull();
    assertThat(result.get("prefix")).isEqualTo(metricPrefix);
    assertThat(result.get("reportTime")).isEqualTo(timestamp);
    assertThat(((Map) result.get("metricValues")).get(metricKey)).isEqualTo(truncatedValue);

  }

  @AfterClass private void exitCluster() {
    kafkaServer.shutdown();
    try {
      zookeeper.close();
    }
    catch (IOException e) {
      LOG.error(e.getMessage());
    }
  }

  private <T extends SpecificRecordBase> T deserialize(byte[] bytes, Schema schema) throws IOException {
    SpecificDatumReader<T> reader = new SpecificDatumReader<T>(schema);
    Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
    return reader.read(null, decoder);
  }
}
