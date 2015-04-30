package com.verisign.storm.metrics.adapters;

import com.verisign.storm.metrics.graphite.GraphiteConnectionFailureException;

import java.io.IOException;

public interface IAdapter {
  public void connect() throws GraphiteConnectionFailureException;

  public void disconnect() throws GraphiteConnectionFailureException;

  public void appendToBuffer(String metricPath, String value, long timestamp);

  public void emptyBuffer();

  public void sendBufferContents() throws IOException;

  public int getFailures();

  public String getServerFingerprint();

}
