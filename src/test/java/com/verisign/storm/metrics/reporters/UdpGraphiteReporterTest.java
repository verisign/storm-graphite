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

import static org.fest.assertions.api.Assertions.assertThat;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.charset.Charset;
import java.util.HashMap;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.verisign.storm.metrics.reporters.graphite.GraphiteReporter;
import com.verisign.storm.metrics.util.ConnectionFailureException;

public class UdpGraphiteReporterTest {

  private DatagramSocket graphiteServer;
  private String graphiteHost;
  private Integer graphitePort;
  private GraphiteReporter graphiteReporter;

  private final Charset DEFAULT_CHARSET = Charsets.UTF_8;

  @BeforeTest
  public void connectClientToGraphiteServer() throws IOException {
    launchGraphiteServer();
    launchGraphiteClient();
  }

  private void launchGraphiteServer() throws IOException {
    graphiteHost = "127.0.0.1";
    graphitePort = 2003;
    graphiteServer = new DatagramSocket(graphitePort);
  }

  private void launchGraphiteClient() throws ConnectionFailureException {
    HashMap<String, Object> reporterConfig = new HashMap<String, Object>();

    reporterConfig.put(GraphiteReporter.GRAPHITE_HOST_OPTION, graphiteHost);
    reporterConfig.put(GraphiteReporter.GRAPHITE_PORT_OPTION, graphitePort.toString());
    reporterConfig.put(GraphiteReporter.GRAPHITE_PROTOCOL_OPTION, "udp");

    graphiteReporter = new GraphiteReporter();
    graphiteReporter.prepare(reporterConfig);
    graphiteReporter.connect();
  }

  @AfterTest
  public void exit() throws IOException {
    if (graphiteServer != null && graphiteServer.isClosed()) {
      graphiteServer.close();
    }
    graphiteReporter.disconnect();
  }

  @DataProvider(name = "metrics")
  public Object[][] metricsProvider() {
    return new Object[][] { new Object[] { "test.storm", "metric1", 1.00, new Long("1408393534971"),
            "test.storm.metric1 1.00 1408393534971\n" },
            new Object[] { "test.storm", "metric2", 0.00, new Long("1408393534971"),
                    "test.storm.metric2 0.00 1408393534971\n" },
            new Object[] { "test.storm", "metric3", 3.14, new Long("1408393534971"),
                    "test.storm.metric3 3.14 1408393534971\n" },
            new Object[] { "test.storm", "metric3", 99.0, new Long("1408393534971"),
                    "test.storm.metric3 99.00 1408393534971\n" },
            new Object[] { "test.storm", "metric3", 1e3, new Long("1408393534971"),
                    "test.storm.metric3 1000.00 1408393534971\n" } };
  }

  @Test(dataProvider = "metrics")
  public void sendMetricTupleAsFormattedStringToGraphiteServer(String metricPrefix, String metricKey, Double value,
                                                               long timestamp,
                                                               String expectedMessageReceived) throws IOException {
    // Given a tuple representing a (metricPath, value, timestamp) metric (injected via data provider)

    HashMap<String, Double> values = new HashMap<String, Double>();
    values.put(metricKey, value);
    // When the reporter sends the metric
    graphiteReporter.appendToBuffer(metricPrefix, values, timestamp);
    graphiteReporter.sendBufferContents();

    // Then the server should receive a properly formatted string representing the metric
    byte[] buf = new byte[256];
    DatagramPacket packet = new DatagramPacket(buf, buf.length);
    graphiteServer.receive(packet);

    String actualMessageReceived = new String(packet.getData(), 0, packet.getLength(), DEFAULT_CHARSET);
    assertThat(actualMessageReceived).isEqualTo(expectedMessageReceived);
  }
}
