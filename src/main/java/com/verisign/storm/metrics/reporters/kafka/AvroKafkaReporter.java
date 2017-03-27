package com.verisign.storm.metrics.reporters.kafka;

import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;

import com.verisign.storm.metrics.serializers.AvroRecordSerializer;

public class AvroKafkaReporter extends BaseKafkaReporter {

  public AvroKafkaReporter() {
    super();
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
@Override public KafkaProducer configureKafkaProducer(Properties producerProps) {
    AvroRecordSerializer serializer = new AvroRecordSerializer();
    return new KafkaProducer<GenericRecord, GenericRecord>(producerProps, serializer, serializer);
  }
}
