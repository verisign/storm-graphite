package com.verisign.storm.metrics.adapters;

import com.verisign.ie.styx.avro.graphingMetrics.GraphingMetrics;
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
  public static final String KAFKA_TOPIC_NAME = "metrics.kafka.topic";
  public static final String KAFKA_BROKER_LIST = "metadata.broker.list";

  private String kafkaTopicName;
  private LinkedList<GraphingMetrics> buffer;

  //KafkaProd
  private Producer<String, byte[]> kafkaProducer;
  DatumWriter<GraphingMetrics> datumWriter;


  @SuppressWarnings("unchecked")
  public KafkaAdapter(Map conf) {
    super(conf);

    if (conf.containsKey(KAFKA_TOPIC_NAME)) {
      kafkaTopicName = (String) conf.get(KAFKA_TOPIC_NAME);
    }
    else {
      throw new IllegalArgumentException("Field " + KAFKA_TOPIC_NAME + " required.");
    }

    if (!conf.containsKey(KAFKA_BROKER_LIST)) {
      throw new IllegalArgumentException("Field " + KAFKA_BROKER_LIST + " required.");
    }

    Properties producerProps = new Properties();

    for (String key : ((Map<String, Object>) conf).keySet()) {
      producerProps.setProperty(key, (String) conf.get(key));
    }

    kafkaProducer = new Producer<String, byte[]>(new ProducerConfig(producerProps));
    datumWriter = new SpecificDatumWriter<GraphingMetrics>(GraphingMetrics.class);
    buffer = new LinkedList<GraphingMetrics>();
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
      if (metrics.get(key) instanceof String) {
        try {
          Double value = Double.parseDouble((String) metrics.get(key));
          metricsDoubleMap.put(key, value);
        }
        catch (NumberFormatException e) {
          //Error out
        }
      }
      else {
        //Error out 
      }
    }

    buffer.add(new GraphingMetrics(prefix, timestamp, metricsDoubleMap));
  }

  @Override public void emptyBuffer() {
    buffer.clear();
  }

  @Override public void sendBufferContents() throws IOException {
    for (GraphingMetrics metric : buffer) {

      byte[] metricBytes = convertGraphingMetricAvroObjectToByteArray(metric);
      KeyedMessage<String, byte[]> message = new KeyedMessage<String, byte[]>(kafkaTopicName, metric.getPrefix(),
          metricBytes);
      kafkaProducer.send(message);
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
    return 0;
  }

  @Override public String getServerFingerprint() {
    return null;
  }
}
