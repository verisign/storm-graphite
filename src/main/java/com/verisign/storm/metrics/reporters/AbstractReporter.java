package com.verisign.storm.metrics.reporters;

import java.io.IOException;
import java.util.Map;

import com.verisign.storm.metrics.util.ConnectionFailureException;

public abstract class AbstractReporter {

  public AbstractReporter() {
  }

  public abstract void prepare(Map<String, Object> conf);
  
  public abstract void connect() throws ConnectionFailureException;

  public abstract void disconnect() throws ConnectionFailureException;

  public abstract void appendToBuffer(String prefix, Map<String, Double> metrics, long timestamp);

  public abstract void emptyBuffer();

  public abstract void sendBufferContents() throws IOException;

  public abstract long getFailures();

  public abstract String getBackendFingerprint();

}
