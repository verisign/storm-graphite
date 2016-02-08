/*
 * Copyright 2014 VeriSign, Inc.
 *
 * VeriSign licenses this file to you under the Apache License, version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 */
package com.verisign.storm.metrics.reporters.graphite;

import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteSender;
import com.codahale.metrics.graphite.GraphiteUDP;
import com.google.common.base.Throwables;
import com.verisign.storm.metrics.reporters.AbstractReporter;
import com.verisign.storm.metrics.util.ConfigurableSocketFactory;
import com.verisign.storm.metrics.util.ConnectionFailureException;
import com.verisign.storm.metrics.util.GraphiteCodec;
import com.verisign.storm.metrics.util.TagsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * This class is a wrapper for the Graphite class in the Metrics library.  It encapsulates the handling of errors that
 * may occur during network communication with Graphite/Carbon.
 */
public class GraphiteReporter extends AbstractReporter {

  private static final Logger LOG = LoggerFactory.getLogger(GraphiteReporter.class);
  private static final int DEFAULT_MIN_CONNECT_ATTEMPT_INTERVAL_SECS = 5;
  private static final int DEFAULT_READ_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(60);
  private static final int DEFAULT_CONNECT_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(60);

  public static final String GRAPHITE_HOST_OPTION = "metrics.graphite.host";
  public static final String GRAPHITE_PORT_OPTION = "metrics.graphite.port";
  public static final String GRAPHITE_PROTOCOL_OPTION = "metrics.graphite.protocol";
  public static final String GRAPHITE_CONNECT_TIMEOUT = "metrics.graphite.connect.timeout";
  public static final String GRAPHITE_READ_TIMEOUT = "metrics.graphite.read.timeout";
  public static final String GRAPHITE_MIN_CONNECT_ATTEMPT_INTERVAL_SECS_OPTION
      = "metrics.graphite.min-connect-attempt-interval-secs";

  private String graphiteHost;
  private int graphitePort;
  private InetSocketAddress graphiteSocketAddr;

  private String serverFingerprint;
  private int minConnectAttemptIntervalSecs;
  private GraphiteSender graphite;
  private long lastConnectAttemptTimestampMs;
  private String prefix;

  public GraphiteReporter() {
    super();
  }

  @Override public void prepare(Map<String, Object> conf) {
    if (conf.containsKey(GRAPHITE_HOST_OPTION)) {
      graphiteHost = (String) conf.get(GRAPHITE_HOST_OPTION);
    }
    else {
      throw new IllegalArgumentException("Field " + GRAPHITE_HOST_OPTION + " required.");
    }

    if (conf.containsKey(GRAPHITE_PORT_OPTION)) {
      graphitePort = Integer.parseInt((String) conf.get(GRAPHITE_PORT_OPTION));
    }
    else {
      throw new IllegalArgumentException("Field " + GRAPHITE_PORT_OPTION + " required.");
    }

    if (conf.containsKey(GRAPHITE_MIN_CONNECT_ATTEMPT_INTERVAL_SECS_OPTION)) {
      minConnectAttemptIntervalSecs = Integer
          .parseInt((String) conf.get(GRAPHITE_MIN_CONNECT_ATTEMPT_INTERVAL_SECS_OPTION));
    }
    else {
      minConnectAttemptIntervalSecs = DEFAULT_MIN_CONNECT_ATTEMPT_INTERVAL_SECS;
    }

    graphiteSocketAddr = new InetSocketAddress(graphiteHost, graphitePort);

    serverFingerprint = graphiteSocketAddr.getAddress() + ":" + graphiteSocketAddr.getPort();

    if (conf.containsKey(GRAPHITE_PROTOCOL_OPTION) && ((String)conf.get(GRAPHITE_PROTOCOL_OPTION)).equalsIgnoreCase("udp")) {
      // Use UDP client
      this.graphite = new GraphiteUDP(graphiteSocketAddr);
    } else {
      // Default TCP client
      int connectTimeout = DEFAULT_CONNECT_TIMEOUT;
      if (conf.containsKey(GRAPHITE_CONNECT_TIMEOUT)) {
        connectTimeout = Integer.parseInt(conf.get(GRAPHITE_CONNECT_TIMEOUT).toString());
      }

      int readTimeout = DEFAULT_READ_TIMEOUT;
      if (conf.containsKey(GRAPHITE_READ_TIMEOUT)) {
        readTimeout = Integer.parseInt(conf.get(GRAPHITE_READ_TIMEOUT).toString());
      }

      ConfigurableSocketFactory socketFactory = new ConfigurableSocketFactory();
      socketFactory.setConnectTimeout(connectTimeout);
      socketFactory.setReadTimeout(readTimeout);

      this.graphite = new Graphite(graphiteSocketAddr, socketFactory);
    }
    lastConnectAttemptTimestampMs = 0;
    prefix = TagsHelper.getPrefix(conf);
  }

  @Override public String getBackendFingerprint() {
    return serverFingerprint;
  }

  @Override public void connect() throws ConnectionFailureException {
    lastConnectAttemptTimestampMs = nowMs();
    try {
      graphite.connect();
    }
    catch (IllegalStateException e) {
      // do nothing, already connected
    }
    catch (IOException e) {
      String msg = "Could not connect to Carbon daemon running at " + serverFingerprint + ": " + e.getMessage();
      LOG.error(msg);
      throw new ConnectionFailureException(msg);
    }
  }

  private long nowMs() {
    return System.currentTimeMillis();
  }

  @Override public void disconnect() {
    try {
      graphite.close();
    }
    catch (IOException e) {
      LOG.error("Could not disconnect from Carbon daemon running at " + serverFingerprint + ": " + e.getMessage());
    }
  }

  @Override public void emptyBuffer() {
  }

  private void appendToBuffer(String prefix, Map<String, Double> metrics, long timestamp) {
    try {
      if(!graphite.isConnected()) {
        graphite.connect();
      }

      for (Map.Entry<String, Double> entry : metrics.entrySet()) {
        graphite.send(prefix + "." + entry.getKey(), GraphiteCodec.format(entry.getValue()), timestamp);
      }
    }
    catch (IOException e) {
      handleFailedSend(e);
    }
    catch (NullPointerException npe) {
      handleFailedSend(npe);
    }
    catch (NumberFormatException nfe) {
      handleFailedSend(nfe);
    }
  }

  @Override
  public void appendToBuffer(Map<String, String> tags, Map<String, Double> metrics, long timestamp) {
    appendToBuffer(TagsHelper.constructMetricPrefix(prefix, tags), metrics, timestamp);
  }

  @Override public void sendBufferContents() throws IOException {
    try {
      if(!graphite.isConnected()) {
        graphite.connect();
      }
      graphite.flush();
    }
    catch (IOException e) {
      handleFailedSend(e);
    }
    catch (NullPointerException npe) {
      handleFailedSend(npe);
    }
  }

  private void handleFailedSend(Exception e) {
    String trace = Throwables.getStackTraceAsString(e);
    LOG.error("Failed to send update to " + serverFingerprint + ": " + e.getMessage() + "\n" + trace);
    if (reconnectingAllowed(nowMs())) {
      try {
        this.disconnect();
        this.connect();
      }
      catch (ConnectionFailureException cf) {
        //Do nothing, error already logged in connect()
      }
    }
    else {
      LOG.warn("Connection attempt limit exceeded to Carbon daemon running at " + serverFingerprint);
    }
  }

  private boolean reconnectingAllowed(long timestampMs) {
    long secondsSinceLastConnectAttempt = (timestampMs - lastConnectAttemptTimestampMs) / 1000;
    return secondsSinceLastConnectAttempt > minConnectAttemptIntervalSecs;
  }


  /**
   * Returns the number of failed writes to the Graphite server.
   *
   * @return the number of failed writes to the Graphite server
   */
  @Override public long getFailures() {
    return graphite.getFailures();
  }

}