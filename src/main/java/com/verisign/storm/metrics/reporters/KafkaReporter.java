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

import com.google.common.base.Throwables;
import com.verisign.ie.styx.avro.graphingMetrics.GraphingMetrics;
import com.verisign.storm.metrics.graphite.GraphiteCodec;
import com.verisign.storm.metrics.graphite.GraphiteConnectionFailureException;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;

public class KafkaReporter extends AbstractReporter {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaReporter.class);
  public static final String KAFKA_TOPIC_NAME_FIELD = "metrics.kafka.topic";
  public static final String KAFKA_BROKER_LIST_FIELD = "metrics.kafka.metadata.broker.list";
  public static final String KAFKA_METADATA_BROKER_LIST = "metadata.broker.list";

  private String kafkaTopicName;
  private String kafkaBrokerList;
  private LinkedList<GraphingMetrics> buffer;

  private Producer<String, byte[]> kafkaProducer;

  private int failures;

  public KafkaReporter(Map conf) {
    super(conf);

    if (conf.containsKey(KAFKA_TOPIC_NAME_FIELD)) {
      kafkaTopicName = (String) conf.get(KAFKA_TOPIC_NAME_FIELD);
    }
    else {
      throw new IllegalArgumentException("Field " + KAFKA_TOPIC_NAME_FIELD + " required.");
    }

    if (conf.containsKey(KAFKA_BROKER_LIST_FIELD)) {
      kafkaBrokerList = (String) conf.get(KAFKA_BROKER_LIST_FIELD);
      conf.put(KAFKA_METADATA_BROKER_LIST, kafkaBrokerList);
    }
    else if (conf.containsKey(KAFKA_METADATA_BROKER_LIST)) {
      kafkaBrokerList = (String) conf.get(KAFKA_METADATA_BROKER_LIST);
    }
    else {
      throw new IllegalArgumentException("Field " + KAFKA_BROKER_LIST_FIELD + " required.");
    }

    Properties producerProps = new Properties();
    for (String key : ((Map<String, Object>) conf).keySet()) {
      producerProps.setProperty(key, (String) conf.get(key));
    }

    kafkaProducer = new Producer<String, byte[]>(new ProducerConfig(producerProps));
    buffer = new LinkedList<GraphingMetrics>();
    failures = 0;
  }

  @Override public void connect() throws GraphiteConnectionFailureException {
  }

  @Override public void disconnect() throws GraphiteConnectionFailureException {
  }

  @Override public void appendToBuffer(String prefix, Map<String, Object> metrics, long timestamp) {
    Map<String, Double> metricsDoubleMap = new HashMap<String, Double>();

    for (String key : metrics.keySet()) {
      try {
        Double value = Double.parseDouble(GraphiteCodec.format(metrics.get(key)));
        metricsDoubleMap.put(key, value);
      }
      catch (NumberFormatException e) {
        String trace = Throwables.getStackTraceAsString(e);
        LOG.error("Error parsing metric value {} in path {}: {}", metrics.get(key), prefix + key, trace);
      }
    }

    buffer.add(new GraphingMetrics(prefix, timestamp, metricsDoubleMap));
  }

  @Override public void emptyBuffer() {
    buffer.clear();
  }

  @Override public void sendBufferContents() throws IOException {
    for (GraphingMetrics metric : buffer) {
      try {
        byte[] metricBytes = serialize(metric);
        kafkaProducer.send(new KeyedMessage<String, byte[]>(kafkaTopicName, metricBytes));
      }
      catch (IOException e) {
        failures++;

        //Pass this exception up to the metrics consumer for it to handle
        throw e;
      }
    }
  }

  @Override public int getFailures() {
    return failures;
  }

  @Override public String getServerFingerprint() {
    return kafkaBrokerList;
  }


  private <T extends SpecificRecordBase> byte[] serialize(T record) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);

    DatumWriter<T> writer = new SpecificDatumWriter<T>(record.getSchema());
    writer.write(record, encoder);
    encoder.flush();
    out.close();
    return out.toByteArray();
  }
}
