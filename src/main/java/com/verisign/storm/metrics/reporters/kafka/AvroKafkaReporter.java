package com.verisign.storm.metrics.reporters.kafka;

import com.verisign.storm.metrics.serializers.AvroRecordSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

public class AvroKafkaReporter extends BaseKafkaReporter {

  public AvroKafkaReporter() {
    super();
  }

  @Override public KafkaProducer configureKafkaProducer(Properties producerProps) {
    AvroRecordSerializer serializer = new AvroRecordSerializer();
    return new KafkaProducer<GenericRecord, GenericRecord>(producerProps, serializer, serializer);
  }
}
