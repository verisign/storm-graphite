package com.verisign.storm.metrics.serializers;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class AvroRecordSerializer implements Serializer<GenericRecord> {
  private static Logger LOG = LoggerFactory.getLogger(AvroRecordSerializer.class);

  public AvroRecordSerializer() {
  }

  @Override public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override public byte[] serialize(String topic, GenericRecord data) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    Schema schema = data.getSchema();


    DatumWriter<GenericRecord> writer = new SpecificDatumWriter<GenericRecord>(schema);
    try {
      writer.write(data, encoder);
      encoder.flush();
      out.close();
      return out.toByteArray();
    }
    catch (IOException e) {
      LOG.error("Error encoding Avro record into bytes: {}", e.getMessage());
      return null;
    }
  }

  @Override public void close() {
  }
}
