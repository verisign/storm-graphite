package com.verisign.storm.metrics.serializers;

import com.verisign.ie.styx.avro.graphingMetrics.GraphingMetrics;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class AvroRecordDeserializer implements Deserializer<GenericRecord> {
  private static Logger LOG = LoggerFactory.getLogger(AvroRecordDeserializer.class);

  @Override public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override public GenericRecord deserialize(String topic, byte[] data) {
    DatumReader<GenericRecord> reader = new SpecificDatumReader<GenericRecord>(GraphingMetrics.getClassSchema());
    try {
      return reader.read(null, DecoderFactory.get().binaryDecoder(data, null));
    }
    catch (IOException e) {
      LOG.error("Error decoding bytes to Avro record: {}", e.getMessage());
      return null;
    }
  }

  @Override public void close() {
  }
}
