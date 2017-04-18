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
package com.verisign.storm.metrics;

import org.apache.storm.metric.api.IMetricsConsumer.DataPoint;
import org.apache.storm.metric.api.IMetricsConsumer.TaskInfo;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;
import com.google.common.collect.Maps;
import com.verisign.storm.metrics.reporters.graphite.GraphiteReporter;
import com.verisign.storm.metrics.reporters.kafka.AvroKafkaReporter;
import com.verisign.storm.metrics.reporters.kafka.BaseKafkaReporter;
import com.verisign.storm.metrics.util.TagsHelper;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.*;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
public class GraphiteMetricsConsumerTest {

  private Logger LOG = org.slf4j.LoggerFactory.getLogger(GraphiteMetricsConsumerTest.class);
  
  private final Long currentTime = System.currentTimeMillis();
  private final Random rng = new Random(currentTime);

  private String testStormComponentID = "testStormComponentID";
  private String testStormSrcWorkerHost = "testStormSrcWorkerHost";
  private Integer testStormSrcWorkerPort = 6700;
  private Integer testStormSrcTaskId = 3008;
  private String testTopologyName = "Example-Storm-Topology-Name-13-1425495763";

  private String testGraphiteHost = "127.0.0.1";
  private Integer testGraphitePort = 2003;
  private InetSocketAddress testGraphiteServerSocketAddr;
  private ServerSocketChannel testGraphiteServer;
  
  private final GraphiteMetricsConsumer consumer = new GraphiteMetricsConsumer();
  private final TaskInfo taskInfo = new TaskInfo(testStormSrcWorkerHost, testStormSrcWorkerPort, testStormComponentID,
      testStormSrcTaskId, currentTime, 10);

  @BeforeClass private void setupTestFixtures() {
    try {
      testGraphiteServerSocketAddr = new InetSocketAddress(testGraphiteHost, testGraphitePort);
      testGraphiteServer = ServerSocketChannel.open();
      testGraphiteServer.socket().bind(testGraphiteServerSocketAddr);
      testGraphiteServer.configureBlocking(false);
    }
    catch (IOException e) {
      LOG.error("Failed to open Graphite server at {}:{}", testGraphiteHost, testGraphitePort);
    }
  }

  @AfterClass private void teardownTestFixtures() {
    try {
      testGraphiteServer.close();
    }
    catch (IOException e) {
      LOG.error("Failed to close Graphite server at {}:{}", testGraphiteHost, testGraphitePort);
    }
  }

  @Test public void shouldInitializeGraphiteReporter() {
    // Given a Graphite configuration and topology context
    Map<String, String> stormConfig = Maps.newHashMap();
    stormConfig.put(GraphiteReporter.GRAPHITE_HOST_OPTION, testGraphiteHost);

    Map<String, String> registrationArgument = Maps.newHashMap();
    registrationArgument.put(GraphiteReporter.GRAPHITE_PORT_OPTION, testGraphitePort.toString());

    TopologyContext topologyContext = mock(TopologyContext.class);
    when(topologyContext.getStormId()).thenReturn(testTopologyName);
    IErrorReporter errorReporter = mock(IErrorReporter.class);

    // When the metrics consumer initializes
    consumer.prepare(stormConfig, registrationArgument, topologyContext, errorReporter);

    // Then the reporter should be initialized with a Graphite reporter
    verify(topologyContext).getStormId();
    assertThat(consumer.getStormId()).isEqualTo(testTopologyName);
    assertThat(consumer.reporter).isInstanceOf(GraphiteReporter.class);
    assertThat(consumer.reporter).isNotNull();
  }

  @Test public void shouldInitializeKafkaReporter() {
    // Given a Kafka configuration and topology context
    Map<String, String> stormConfig = Maps.newHashMap();
    stormConfig.put(BaseKafkaReporter.KAFKA_BROKER_LIST_FIELD, "127.0.0.1:9092");
    stormConfig.put(GraphiteMetricsConsumer.REPORTER_NAME, AvroKafkaReporter.class.getName());

    Map<String, String> registrationArgument = Maps.newHashMap();
    registrationArgument.put(BaseKafkaReporter.KAFKA_TOPIC_NAME_FIELD, "testTopic");

    TopologyContext topologyContext = mock(TopologyContext.class);
    when(topologyContext.getStormId()).thenReturn(testTopologyName);
    IErrorReporter errorReporter = mock(IErrorReporter.class);

    // When the metrics consumer initializes
    consumer.prepare(stormConfig, registrationArgument, topologyContext, errorReporter);

    // Then the reporter should be initialized with a Kafka reporter
    verify(topologyContext).getStormId();
    assertThat(consumer.getStormId()).isEqualTo(testTopologyName);
    assertThat(consumer.reporter).isInstanceOf(BaseKafkaReporter.class);
    assertThat(consumer.reporter).isNotNull();
  }

  @Test(dependsOnMethods = { "shouldInitializeGraphiteReporter" }, dataProvider = "generateMapDataPoints")
  public void shouldReadDataAndSendMapDataPoints(Collection<DataPoint> dataPoints) {
    //Given a consumer
    GraphiteMetricsConsumer consumer = spy(new GraphiteMetricsConsumer());

    Map<String, String> stormConfig = Maps.newHashMap();
    stormConfig.put(GraphiteReporter.GRAPHITE_HOST_OPTION, testGraphiteHost);
    stormConfig.put(GraphiteReporter.GRAPHITE_PORT_OPTION, testGraphitePort.toString());
    stormConfig.put(GraphiteMetricsConsumer.REPORTER_NAME, GraphiteReporter.class.getName());

    HashMap<String, Object> registrationArgs = new HashMap<String, Object>();
    TopologyContext context = mock(TopologyContext.class);
    when(context.getStormId()).thenReturn(testTopologyName);
    IErrorReporter errorReporter = mock(IErrorReporter.class);
    consumer.prepare(stormConfig, registrationArgs, context, errorReporter);
    doNothing().when(consumer).graphiteConnect();

    // and a collection of data points containing Integer data (already injected via data provider)
    // When the reporter processes each data point
    consumer.handleDataPoints(taskInfo, dataPoints);

    // Then the reporter should send properly formatted metric messages to Graphite
    HashMap<String, Double> expMap = buildExpectedMetricMap(dataPoints);
    verify(consumer).appendToReporterBuffer(TagsHelper.convertToTags(testTopologyName, taskInfo), expMap, taskInfo.timestamp);
    String test = "";
  }

  @DataProvider(name = "generateMapDataPoints") public Object[][] generateMapDataPoints() {
    int numDataPoints = 5;
    int numDataPointValues = 5;

    Object[][] testData = new Object[numDataPoints][1];

    for (int i = 0; i < numDataPoints; i++) {
      Collection<DataPoint> dpList = new ArrayList<DataPoint>();
      HashMap<String, Object> dpMap = new HashMap<String, Object>();

      for (int j = 0; j < numDataPointValues; j++) {
        dpMap.put(RandomStringUtils.randomAlphanumeric(10), rng.nextInt());
        dpMap.put(RandomStringUtils.randomAlphanumeric(10), Integer.toString(rng.nextInt()));

        dpMap.put(RandomStringUtils.randomAlphanumeric(10), rng.nextLong());
        dpMap.put(RandomStringUtils.randomAlphanumeric(10), Long.toString(rng.nextLong()));

        dpMap.put(RandomStringUtils.randomAlphanumeric(10), rng.nextFloat());
        dpMap.put(RandomStringUtils.randomAlphanumeric(10), Float.toString(rng.nextFloat()));

        dpMap.put(RandomStringUtils.randomAlphanumeric(10), rng.nextDouble());
        dpMap.put(RandomStringUtils.randomAlphanumeric(10), Double.toString(rng.nextDouble()));

        dpMap.put(RandomStringUtils.randomAlphanumeric(10), RandomStringUtils.randomAlphanumeric(10));
        dpMap.put(RandomStringUtils.randomAlphanumeric(10), null);
        dpMap.put(RandomStringUtils.randomAlphanumeric(10), Integer.toString(rng.nextInt()));
      }
      DataPoint dp = new DataPoint(RandomStringUtils.randomAlphanumeric(10), dpMap);
      dpList.add(dp);
      testData[i][0] = dpList;
    }

    return testData;
  }

  private HashMap<String, Double> buildExpectedMetricMap(Collection<DataPoint> dataPoints) {
    HashMap<String, Double> expMap = new HashMap<String, Double>();
    for (DataPoint dp : dataPoints) {

      Map<String, Object> datamap = (Map<String, Object>) dp.value;

      for (String key : datamap.keySet()) {

        if (datamap.get(key) != null) {
          if (datamap.get(key) instanceof String) {
            try {
              expMap.put(dp.name + "." + key, Double.parseDouble((String) datamap.get(key)));
            }
            catch (NumberFormatException e) {
              // Do not add invalid strings to expected output
            }
          }
          else {
            expMap.put(dp.name + "." + key, ((Number) datamap.get(key)).doubleValue());
          }
        }
        else {
          // Do not add null values to expected output
        }
      }
    }
    return expMap;
  }
}