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
package com.verisign.storm.metrics.reporters;

import com.codahale.metrics.graphite.Graphite;
import com.google.common.base.Throwables;
import com.verisign.storm.metrics.graphite.GraphiteCodec;
import com.verisign.storm.metrics.graphite.GraphiteConnectionFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

/**
 * This class is a wrapper for the Graphite class in the Metrics library.  It encapsulates the handling of errors that
 * may occur during network communication with Graphite/Carbon.
 */
public class GraphiteReporter extends AbstractReporter {

  private static final Logger LOG = LoggerFactory.getLogger(GraphiteReporter.class);
  private static final int DEFAULT_MIN_CONNECT_ATTEMPT_INTERVAL_SECS = 5;
  public static final String GRAPHITE_HOST_OPTION = "metrics.graphite.host";
  public static final String GRAPHITE_PORT_OPTION = "metrics.graphite.port";
  public static final String GRAPHITE_MIN_CONNECT_ATTEMPT_INTERVAL_SECS_OPTION
      = "metrics.graphite.min-connect-attempt-interval-secs";


  private String graphiteHost;
  private int graphitePort;
  private InetSocketAddress graphiteSocketAddr;

  private final String serverFingerprint;
  private final int minConnectAttemptIntervalSecs;
  private final Graphite graphite;
  private long lastConnectAttemptTimestampMs;

  public static AbstractReporter newAdapter(Map conf) {
    return new GraphiteReporter(conf);

  }

  public GraphiteReporter(Map<String, Object> conf) {
    super(conf);

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
    this.graphite = new Graphite(graphiteSocketAddr);
    lastConnectAttemptTimestampMs = 0;

  }

  @Override public String getServerFingerprint() {
    return serverFingerprint;
  }

  @Override public void connect() throws GraphiteConnectionFailureException {
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
      throw new GraphiteConnectionFailureException(msg);
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
    return;
  }



  @Override public void appendToBuffer(String prefix, Map<String, Object> metrics, long timestamp) {
    try {
      if(!graphite.isConnected()) {
        graphite.connect();
      }

      for (String key : metrics.keySet()) {
        graphite.send(prefix + "." + key, GraphiteCodec.format(metrics.get(key)), timestamp);
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
      catch (GraphiteConnectionFailureException cf) {
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
  @Override public int getFailures() {
    return graphite.getFailures();
  }

}