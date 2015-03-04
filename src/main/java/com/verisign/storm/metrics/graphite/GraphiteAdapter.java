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
package com.verisign.storm.metrics.graphite;

import com.codahale.metrics.graphite.Graphite;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * This class is a wrapper for the Graphite class in the Metrics library.  It encapsulates the handling of errors that
 * may occur during network communication with Graphite/Carbon.
 */
public class GraphiteAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(GraphiteAdapter.class);
  private static final int DEFAULT_MIN_CONNECT_ATTEMPT_INTERVAL_SECS = 5;
  private final String serverFingerprint;
  private final int minConnectAttemptIntervalSecs;
  private final Graphite graphite;
  private long lastConnectAttemptTimestampMs;

  public GraphiteAdapter(InetSocketAddress server) {
    this(server, DEFAULT_MIN_CONNECT_ATTEMPT_INTERVAL_SECS);
  }

  public GraphiteAdapter(InetSocketAddress server, int minConnectAttemptIntervalSecs) {
    if (server == null) {
      throw new IllegalArgumentException("server address must not be null");
    }
    serverFingerprint = server.getAddress() + ":" + server.getPort();
    this.minConnectAttemptIntervalSecs = minConnectAttemptIntervalSecs;
    this.graphite = new Graphite(server);
    lastConnectAttemptTimestampMs = 0;
  }

  public String getServerFingerprint() {
    return serverFingerprint;
  }

  public void connect() throws GraphiteConnectionAttemptFailure {
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
      throw new GraphiteConnectionAttemptFailure(msg);
    }
  }

  private long nowMs() {
    return System.currentTimeMillis();
  }

  public void disconnect() {
    try {
      graphite.close();
    }
    catch (IOException e) {
      LOG.error("Could not disconnect from Carbon daemon running at " + serverFingerprint + ": " + e.getMessage());
    }
  }

  public void appendToSendBuffer(String metricPath, String value, long timestamp) {
    try {
      if(!graphite.isConnected()) {
        graphite.connect();
      }
      graphite.send(metricPath, value, timestamp);
    }
    catch (IOException e) {
      handleFailedSend(e);
    }
    catch (NullPointerException npe) {
      handleFailedSend(npe);
    }
  }

  public void flushSendBuffer() throws IOException {
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
      catch (GraphiteConnectionAttemptFailure cf) {
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
  public int getFailures() {
    return graphite.getFailures();
  }

}