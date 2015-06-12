package com.verisign.storm.metrics.reporters.kafka;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

public class SchemaRegistryKafkaReporter extends BaseKafkaReporter {

  String KAFKA_SCHEMA_REGISTRY_URL_FIELD = "metrics.kafka.schema.registry.url";
  String KAFKA_SCHEMA_REGISTRY_IDENTITY_MAP_CAPACITY_FIELD = "metrics.kafka.schema.registry.id.capacity";

  public SchemaRegistryKafkaReporter() {
    super();
  }

  @Override public KafkaProducer configureKafkaProducer(Properties producerProps) {
    String schemaRegistryUrl;
    Integer identityMapCapacity;

    if (producerProps.containsKey(KAFKA_SCHEMA_REGISTRY_URL_FIELD)) {
      schemaRegistryUrl = (String) producerProps.get(KAFKA_SCHEMA_REGISTRY_URL_FIELD);
    }
    else {
      throw new IllegalArgumentException("Field " + KAFKA_SCHEMA_REGISTRY_URL_FIELD + " required.");
    }

    if (producerProps.containsKey(KAFKA_SCHEMA_REGISTRY_IDENTITY_MAP_CAPACITY_FIELD)) {
      identityMapCapacity = (Integer) producerProps.get(KAFKA_SCHEMA_REGISTRY_IDENTITY_MAP_CAPACITY_FIELD);
    }
    else {
      identityMapCapacity = 100;
    }

    CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(schemaRegistryUrl, identityMapCapacity);
    KafkaAvroSerializer serializer = new KafkaAvroSerializer(client);
    return new KafkaProducer<Object, Object>(producerProps, serializer, serializer);
  }
}
