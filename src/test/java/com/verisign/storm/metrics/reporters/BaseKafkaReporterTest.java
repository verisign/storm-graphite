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
import com.verisign.storm.metrics.util.TagsHelper;
import io.confluent.kafka.schemaregistry.client.LocalSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.Message;
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

public class BaseKafkaReporterTest {
  private Logger LOG = LoggerFactory.getLogger(BaseKafkaReporterTest.class);

  private String testZkHost = "127.0.0.1";
  private Integer testZkPort = 2181;
  private String testZkConnect = testZkHost + ":" + testZkPort;

  private String testKafkaHost = "127.0.0.1";
  private Integer testKafkaPort = 9092;
  private String testKafkaBrokerList = testKafkaHost + ":" + testKafkaPort;

  private String destinationTopic;
  private KafkaServerStartable kafkaServer;
  private TestingServer zookeeper;
  private BaseKafkaReporter kafkaReporter;
  private LocalSchemaRegistryClient schemaRegistryClient;

  @BeforeClass private void initializeCluster() {
    try {
      zookeeper = new TestingServer(testZkPort);
    }
    catch (Exception e) {
      LOG.error(e.getMessage());
    }

    KafkaConfig kafkaConfig = new KafkaConfig(getBrokerConfig());
    kafkaServer = new KafkaServerStartable(kafkaConfig);
    kafkaServer.startup();
  }

  @AfterClass private void exitCluster() {
    kafkaServer.shutdown();
    try {
      zookeeper.close();
    }
    catch (IOException e) {
      LOG.error("Failed to close Zookeeper server at {}:{}", testZkHost, testZkPort);
    }
  }


  @Test(dataProvider = "kafkaMetrics")
  public void avroKafkaReporterTest(HashMap<String, String> tags, String metricKey, Double value, Double truncatedValue,
      long timestamp) {

    /* GIVEN: A Zookeeper instance, a Kafka broker, and a the Kafka reporter we're testing */
    initializeAvroReporter();
    SimpleConsumer kafkaConsumer = new SimpleConsumer(testKafkaHost, testKafkaPort, 10000, 1024000, "simpleConsumer");
    
    /* WHEN: A new metric is appended to the reporter's buffer and we tell the reporter to send its data */
    submitMetricToReporter(tags, metricKey, value, timestamp);

    /* WHEN: A Kafka consumer reads the latest message from the same topic on the Kafka server */
    byte[] bytes = fetchLatestRecordPayloadBytes(kafkaConsumer);
    
    /* WHEN: The latest message is decoded using the Schema Regsitry based decoder */
    GenericRecord result = null;
    try {
      result = deserialize(bytes, GraphingMetrics.getClassSchema());
    }
    catch (IOException e) {
      fail("Failed to deserialize message:" + e.getMessage());
    }

    /* THEN: The field values of the decoded record should be the same as those of the input fields. */
    assertThat(result).isNotNull();
    assertThat(result.get("prefix")).isEqualTo(TagsHelper.constructMetricPrefix(TagsHelper.DEFAULT_PREFIX, tags));
    assertThat(result.get("reportTime")).isEqualTo(timestamp);
    assertThat(((Map) result.get("metricValues")).get(metricKey)).isEqualTo(truncatedValue);
  }


  @Test(dataProvider = "kafkaMetrics")
  public void schemaRegistryKafkaReporterTest(HashMap<String,String> tags, String metricKey, Double value,
      Double truncatedValue,
      long timestamp) {

    /* GIVEN: A Zookeeper instance, a Kafka broker, and a the Schema Registry-based Kafka reporter we're testing */
    initializeSchemaRegistryReporter();
    SimpleConsumer kafkaConsumer = new SimpleConsumer(testKafkaHost, testKafkaPort, 10000, 1024000, "simpleConsumer");
    KafkaAvroDecoder decoder = new KafkaAvroDecoder(schemaRegistryClient);

    /* WHEN: A new metric is appended to the reporter's buffer and we tell the reporter to send its data */
    submitMetricToReporter(tags, metricKey, value, timestamp);

    /* WHEN: A Kafka consumer reads the latest message from the same topic on the Kafka server */
    byte[] bytes = fetchLatestRecordPayloadBytes(kafkaConsumer);
    
    /* WHEN: The latest message is decoded using the Schema Regsitry based decoder */
    GenericRecord result = null;
    try {
      result = (GenericRecord) decoder.fromBytes(bytes);
    }
    catch (SerializationException e) {
      fail("Failed to deserialize message:" + e.getMessage());
    }

    /* THEN: The field values of the decoded record should be the same as those of the input fields. */
    assertThat(result).isNotNull();
    assertThat(result.get("prefix")).isEqualTo(TagsHelper.constructMetricPrefix(TagsHelper.DEFAULT_PREFIX, tags));
    assertThat(result.get("reportTime")).isEqualTo(timestamp);
    assertThat(((Map) result.get("metricValues")).get(metricKey)).isEqualTo(truncatedValue);
  }

  private void submitMetricToReporter(HashMap<String,String> tags, String metricKey, Double value, long timestamp) {
    HashMap<String, Double> metrics = new HashMap<String, Double>();
    metrics.put(metricKey, value);

    kafkaReporter.appendToBuffer(tags, metrics, timestamp);
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
  }

  private byte[] fetchLatestRecordPayloadBytes(SimpleConsumer kafkaConsumer) {
    FetchRequest fetchRequest = new FetchRequestBuilder().addFetch(destinationTopic, 0, 0, 1000000).build();
    FetchResponse response = kafkaConsumer.fetch(fetchRequest);

    Iterator<MessageAndOffset> messageSetItr = response.messageSet(destinationTopic, 0).iterator();

    // Fast forward to the message at the latest offset in the topic
    MessageAndOffset latestMessage = new MessageAndOffset(new Message(new byte[] { }), 0L);
    while (messageSetItr.hasNext()) {
      latestMessage = messageSetItr.next();
    }

    ByteBuffer payload = latestMessage.message().payload();
    byte[] bytes = new byte[payload.limit()];
    payload.get(bytes);
    return bytes;
  }

  private Properties getBrokerConfig() {
    Properties props = new Properties();
    props.put("broker.id", "0");
    props.put("host.name", testKafkaHost);
    props.put("port", testKafkaPort.toString());
    props.put("num.partitions", "1");
    props.put("auto.create.topics.enable", "true");
    props.put("message.max.bytes", "1000000");
    props.put("zookeeper.connect", testZkConnect);

    return props;
  }

  private void initializeSchemaRegistryReporter() {
    destinationTopic = "schemaRegistryDestinationTopic";
    HashMap<String, Object> reporterConfig = new HashMap<String, Object>();
    reporterConfig.put(BaseKafkaReporter.KAFKA_BROKER_LIST_FIELD, testKafkaBrokerList);
    reporterConfig.put(BaseKafkaReporter.KAFKA_TOPIC_NAME_FIELD, destinationTopic);
    reporterConfig.put("parallelism.hint", 1);
    reporterConfig.put("some.value.that.is.null", null);

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

    kafkaReporter = new BaseKafkaReporter() {
      @Override public KafkaProducer configureKafkaProducer(Properties producerProps) {
        return new KafkaProducer<Object, Object>(producerProps, serializer, serializer);
      }
    };
    kafkaReporter.prepare(reporterConfig);

  }

  private void initializeAvroReporter() {
    destinationTopic = "avroDestinationTopic";
    HashMap<String, Object> reporterConfig = new HashMap<String, Object>();
    reporterConfig.put(BaseKafkaReporter.KAFKA_BROKER_LIST_FIELD, testKafkaBrokerList);
    reporterConfig.put(BaseKafkaReporter.KAFKA_TOPIC_NAME_FIELD, destinationTopic);
    reporterConfig.put("parallelism.hint", 1);
    reporterConfig.put("some.value.that.is.null", null);

    kafkaReporter = new BaseKafkaReporter() {
      @Override public KafkaProducer configureKafkaProducer(Properties producerProps) {
        AvroRecordSerializer serializer = new AvroRecordSerializer();
        return new KafkaProducer<GenericRecord, GenericRecord>(producerProps, serializer, serializer);
      }
    };
    kafkaReporter.prepare(reporterConfig);
  }

  private <T extends SpecificRecordBase> T deserialize(byte[] bytes, Schema schema) throws IOException {
    SpecificDatumReader<T> reader = new SpecificDatumReader<T>(schema);
    Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
    return reader.read(null, decoder);
  }
  @DataProvider(name = "kafkaMetrics")
  public Object[][] kafkaMetricsProvider() {
    return new ReporterDataProvider().kafkaMetricsProvider();
  }

}
