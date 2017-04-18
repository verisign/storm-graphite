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

import com.google.common.base.Charsets;
import com.verisign.storm.metrics.reporters.graphite.GraphiteReporter;
import com.verisign.storm.metrics.util.ConnectionFailureException;
import com.verisign.storm.metrics.util.TagsHelper;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.HashMap;

import static org.fest.assertions.api.Assertions.assertThat;

public class GraphiteReporterTest {

  private ServerSocketChannel graphiteServer;
  private String graphiteHost;
  private Integer graphitePort;
  private InetSocketAddress graphiteSocketAddress;
  private GraphiteReporter graphiteReporter;
  private SocketChannel socketChannel;

  private final Charset DEFAULT_CHARSET = Charsets.UTF_8;

  @BeforeTest
  public void connectClientToGraphiteServer() throws IOException {
    launchGraphiteServer();
    launchGraphiteClient();
    acceptClientConnection();
  }

  private void launchGraphiteServer() throws IOException {
    graphiteHost = "127.0.0.1";
    graphitePort = 2003;

    graphiteSocketAddress = new InetSocketAddress(graphiteHost, graphitePort);
    graphiteServer = ServerSocketChannel.open();
    graphiteServer.socket().bind(graphiteSocketAddress);
    graphiteServer.configureBlocking(false);
  }

  private void launchGraphiteClient() throws ConnectionFailureException {
    HashMap<String, Object> reporterConfig = new HashMap<String, Object>();

    reporterConfig.put(GraphiteReporter.GRAPHITE_HOST_OPTION, graphiteHost);
    reporterConfig.put(TagsHelper.GRAPHITE_PREFIX_OPTION, ReporterDataProvider.testPrefix);
    reporterConfig.put(GraphiteReporter.GRAPHITE_PORT_OPTION, graphitePort.toString());

    graphiteReporter = new GraphiteReporter();
    graphiteReporter.prepare(reporterConfig);
    graphiteReporter.connect();
  }

  private void acceptClientConnection() throws IOException {
    socketChannel = graphiteServer.accept();
  }

  @AfterTest
  public void exit() throws IOException {
    if (graphiteServer != null && graphiteServer.isOpen()) {
      graphiteServer.close();
    }
    graphiteReporter.disconnect();
  }

  @Test(dataProvider = "metrics")
  public void sendMetricTupleAsFormattedStringToGraphiteServer(HashMap<String,String> tags, String metricKey, Double value,
      long timestamp,
      String expectedMessageReceived) throws IOException {
    // Given a tuple representing a (metricPath, value, timestamp) metric (injected via data provider)

    HashMap<String, Double> values = new HashMap<String, Double>();
    values.put(metricKey, value);
    // When the reporter sends the metric
    graphiteReporter.appendToBuffer(tags, values, timestamp);
    graphiteReporter.sendBufferContents();

    // Then the server should receive a properly formatted string representing the metric
    ByteBuffer receive = ByteBuffer.allocate(1024);
    int bytesRead = socketChannel.read(receive);
    String actualMessageReceived = new String(receive.array(), 0, bytesRead, DEFAULT_CHARSET);
    assertThat(actualMessageReceived).isEqualTo(expectedMessageReceived);
  }

  @DataProvider(name = "metrics")
  public Object[][] metricsProvider() {
    return new ReporterDataProvider().metricsProvider();
  }

}
