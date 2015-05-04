package com.verisign.storm.metrics.adapters;

import com.verisign.storm.metrics.graphite.GraphiteConnectionFailureException;

import java.io.IOException;
import java.util.Map;

public class KafkaAdapter extends AbstractAdapter {
  public KafkaAdapter(Map conf) {
    super(conf);
  }

  @Override public void connect() throws GraphiteConnectionFailureException {

  }

  @Override public void disconnect() throws GraphiteConnectionFailureException {

  }

  @Override public void appendToBuffer(String metricPath, String value, long timestamp) {

  }

  @Override public void emptyBuffer() {

  }

  @Override public void sendBufferContents() throws IOException {

  }

  @Override public int getFailures() {
    return 0;
  }

  @Override public String getServerFingerprint() {
    return null;
  }
}
