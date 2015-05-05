package com.verisign.storm.metrics.adapters;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;

public class KafkaAdapter extends AbstractAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaAdapter.class);
  public static final String KAFKA_TOPIC_NAME_FIELD = "metrics.kafka.topic";
  public static final String KAFKA_BROKER_LIST_FIELD = "metadata.broker.list";

  private String kafkaTopicName;
  private String kafkaBrokerList;
  private LinkedList<GraphingMetrics> buffer;

  private Producer<String, byte[]> kafkaProducer;
  DatumWriter<GraphingMetrics> datumWriter;

  private int failures;
  
  public KafkaAdapter(Map conf) {
    super(conf);

    if (conf.containsKey(KAFKA_TOPIC_NAME_FIELD)) {
      kafkaTopicName = (String) conf.get(KAFKA_TOPIC_NAME_FIELD);
    }
    else {
      throw new IllegalArgumentException("Field " + KAFKA_TOPIC_NAME_FIELD + " required.");
    }

    if (conf.containsKey(KAFKA_BROKER_LIST_FIELD)) {
      kafkaBrokerList = (String) conf.get(KAFKA_BROKER_LIST_FIELD);
    }
    else {
      throw new IllegalArgumentException("Field " + KAFKA_BROKER_LIST_FIELD + " required.");
    }

    Properties producerProps = new Properties();
    for (String key : ((Map<String, Object>) conf).keySet()) {
      producerProps.setProperty(key, (String) conf.get(key));
    }

    kafkaProducer = new Producer<String, byte[]>(new ProducerConfig(producerProps));
    datumWriter = new SpecificDatumWriter<GraphingMetrics>(GraphingMetrics.class);
    buffer = new LinkedList<GraphingMetrics>();
    failures = 0;
  }

  @Override public void connect() throws GraphiteConnectionFailureException {
    return;
  }

  @Override public void disconnect() throws GraphiteConnectionFailureException {
    return;
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
        byte[] metricBytes = convertGraphingMetricAvroObjectToByteArray(metric);
        KeyedMessage<String, byte[]> message = new KeyedMessage<String, byte[]>(kafkaTopicName, metric.getPrefix(),
            metricBytes);
        kafkaProducer.send(message);
      }
      catch (IOException e) {
        failures++;

        //Pass this exception up to the metrics consumer for it to handle
        throw e;
      }
    }
  }

  private byte[] convertGraphingMetricAvroObjectToByteArray(GraphingMetrics graphingMetrics) throws IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(stream, null);
    datumWriter.write(graphingMetrics, encoder);
    encoder.flush();
    return stream.toByteArray();
  }

  @Override public int getFailures() {
    return failures;
  }

  @Override public String getServerFingerprint() {
    return kafkaBrokerList;
  }
}
