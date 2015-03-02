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

import com.google.common.base.Charsets;
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

import static org.fest.assertions.api.Assertions.assertThat;

public class GraphiteAdapterTest {

  private ServerSocketChannel graphiteServer;
  private InetSocketAddress graphiteSocketAddress;
  private GraphiteAdapter testAdapter;
  private SocketChannel socketChannel;

  private final Charset DEFAULT_CHARSET = Charsets.UTF_8;

  @BeforeTest
  public void connectClientToGraphiteServer() throws IOException {
    launchGraphiteServer();
    launchGraphiteClient();
    acceptClientConnection();
  }

  private void launchGraphiteServer() throws IOException {
    String ANY_GRAPHITE_HOST = "127.0.0.1";
    int ANY_GRAPHITE_PORT = 2003;
    graphiteSocketAddress = new InetSocketAddress(ANY_GRAPHITE_HOST, ANY_GRAPHITE_PORT);
    graphiteServer = ServerSocketChannel.open();
    graphiteServer.socket().bind(graphiteSocketAddress);
    graphiteServer.configureBlocking(false);
  }

  private void launchGraphiteClient() throws GraphiteConnectionAttemptFailure {
    testAdapter = new GraphiteAdapter(graphiteSocketAddress);
    testAdapter.connect();
  }

  private void acceptClientConnection() throws IOException {
    socketChannel = graphiteServer.accept();
  }

  @AfterTest
  public void exit() throws IOException {
    if (graphiteServer != null && graphiteServer.isOpen()) {
      graphiteServer.close();
    }
    testAdapter.disconnect();
  }

  @DataProvider(name = "metrics")
  public Object[][] metricsProvider() {
    return new Object[][] { new Object[] { "test.storm.metric1", "1.00", new Long("1408393534971"),
        "test.storm.metric1 1.00 1408393534971\n" },
        new Object[] { "test.storm.metric2", "0.00", new Long("1408393534971"),
            "test.storm.metric2 0.00 1408393534971\n" },
        new Object[] { "test.storm.metric3", "3.14", new Long("1408393534971"),
            "test.storm.metric3 3.14 1408393534971\n" } };
  }

  @Test(dataProvider = "metricsProvider")
  public void sendMetricTupleAsFormattedStringToGraphiteServer(String metricPath, String value, long timestamp,
      String expectedMessageReceived) throws IOException {
    // Given a tuple representing a (metricPath, value, timestamp) metric (injected via data provider)

    // When the adapter sends the metric
    testAdapter.appendToSendBuffer(metricPath, value, timestamp);

    // Then the server should receive a properly formatted string representing the metric
    ByteBuffer receive = ByteBuffer.allocate(1024);
    int bytesRead = socketChannel.read(receive);
    String actualMessageReceived = new String(receive.array(), 0, bytesRead, DEFAULT_CHARSET);
    assertThat(actualMessageReceived).isEqualTo(expectedMessageReceived);
  }

}
