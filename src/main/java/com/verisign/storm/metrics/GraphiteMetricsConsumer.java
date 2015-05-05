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

import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import com.google.common.base.Throwables;
import com.verisign.storm.metrics.adapters.AbstractAdapter;
import com.verisign.storm.metrics.graphite.GraphiteConnectionFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Listens for all of Storm's built-in metrics and forwards them to a Graphite server.
 *
 * To use, add this to your topology's configuration:
 *
 * <pre>
 * {@code
 *   conf.registerMetricsConsumer(com.verisign.storm.metrics.GraphiteMetricsConsumer.class, 1);
 *   conf.put("metrics.graphite.host", "<GRAPHITE HOSTNAME>");
 *   conf.put("metrics.graphite.port", "<GRAPHITE PORT>");
 *   conf.put("metrics.graphite.prefix", "<DOT DELIMITED PREFIX>");
 *   // Optional settings
 *   conf.put("metrics.graphite.min-connect-attempt-interval-secs", "5");
 * }
 * </pre>
 *
 * Or edit the storm.yaml config file:
 *
 * <pre>
 * {@code
 *   topology.metrics.consumer.register:
 *     - class: "backtype.storm.metrics.GraphiteMetricsConsumer"
 *       parallelism.hint: 1
 *   metrics.graphite.host: "<GRAPHITE HOSTNAME>"
 *   metrics.graphite.port: "<GRAPHITE PORT>"
 *   metrics.graphite.prefix: "<DOT DELIMITED PREFIX>"
 *   metrics.graphite.min-connect-attempt-interval-secs: "5"
 * }
 * </pre>
 */
public class GraphiteMetricsConsumer implements IMetricsConsumer {

  private static final String GRAPHITE_PREFIX_OPTION = "metrics.graphite.prefix";
  private static final String REPORTER_NAME = "metrics.reporter.name";
  private static final Logger LOG = LoggerFactory.getLogger(GraphiteMetricsConsumer.class);
  private static final String DEFAULT_PREFIX = "metrics";

  private String graphitePrefix;
  private String stormId;
  private AbstractAdapter adapter;
  private Map reporterConfig;

  protected String getStormId() {
    return stormId;
  }

  protected String getGraphitePrefix() {
    return graphitePrefix;
  }

  @Override
  public void prepare(Map config, Object registrationArgument, TopologyContext context, IErrorReporter errorReporter) {

    Map configMap = new HashMap();
    configMap.putAll(config);
    if (registrationArgument instanceof Map) {
      configMap.putAll((Map) registrationArgument);
    }
    reporterConfig = configMap;

    if (reporterConfig.containsKey(GRAPHITE_PREFIX_OPTION)) {
      graphitePrefix = (String) reporterConfig.get(GRAPHITE_PREFIX_OPTION);
    }
    else {
      graphitePrefix = DEFAULT_PREFIX;
    }

    try {
      adapter = configureReporter(reporterConfig);
    }
    catch (Exception e) {
      errorReporter.reportError(e);
    }

    stormId = context.getStormId();
  }

  private AbstractAdapter configureReporter(@SuppressWarnings("rawtypes") Map conf)
      throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException,
      InvocationTargetException {
    String className = (String) conf.get(REPORTER_NAME);
    Class reporterClass = Class.forName(className);
    Constructor ctor = reporterClass.getConstructor(conf.getClass());
    AbstractAdapter adapter = (AbstractAdapter) ctor.newInstance(conf);

    return adapter;
  }

  @Override @SuppressWarnings("unchecked")
  public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
    graphiteConnect();
    emptyBuffer();
    String metricPrefix = constructMetricPrefix(graphitePrefix, taskInfo);

    for (DataPoint dataPoint : dataPoints) {
      // TODO: Correctly process metrics of the messaging layer queues and connection states.
      // These metrics need to be handled differently as they are more structured than raw numbers.
      // For the moment we ignore these metrics and track this task at
      // https://github.com/verisign/storm-graphite/issues/2.
      if (dataPoint.name.equalsIgnoreCase("__send-iconnection") ||
          dataPoint.name.equalsIgnoreCase("__recv-iconnection")) {
        continue;
      }

      // Most data points contain a Map as a value.
      if (dataPoint.value instanceof Map) {
        Map<String, Object> m = (Map<String, Object>) dataPoint.value;
        if (!m.isEmpty()) {
          appendToBuffer(metricPrefix, m, taskInfo.timestamp);
        }
      }
      else {
        HashMap<String, Object> metric = new HashMap<String, Object>();
        metric.put(dataPoint.name + "value", dataPoint.value);
        appendToBuffer(metricPrefix, metric, taskInfo.timestamp);
      }
    }
    sendMetrics();
    graphiteDisconnect();
  }

  /**
   * Constructs a fully qualified metric prefix.
   *
   * @param taskInfo  The information regarding the context in which the data point is supplied
   *
   * @return A fully qualified metric prefix.
   */
  private String constructMetricPrefix(String prefixFromConfig, TaskInfo taskInfo) {
    StringBuilder sb = new StringBuilder();
    if (prefixFromConfig != null && !prefixFromConfig.isEmpty()) {
      sb.append(prefixFromConfig).append(".");
    }
    sb.append(removeNonce(stormId)).append(".");
    sb.append(taskInfo.srcComponentId).append(".");
    sb.append(taskInfo.srcWorkerHost).append(".");
    sb.append(taskInfo.srcWorkerPort).append(".");
    sb.append(taskInfo.srcTaskId).append(".");
    return sb.toString();
  }

  /**
   * Removes nonce appended to topology name (e.g. "Example-Topology-1-2345" -> "Example-Topology")
   */
  private String removeNonce(String topologyId) {
    return topologyId.substring(0, topologyId.substring(0, topologyId.lastIndexOf("-")).lastIndexOf("-"));
  }

  protected void graphiteConnect() {
    try {
      adapter.connect();
    }
    catch (GraphiteConnectionFailureException e) {
      String trace = Throwables.getStackTraceAsString(e);
      LOG.error("Could not connect to adapter backend " + adapter.getServerFingerprint() + ": " + trace);
    }
  }

  protected void appendToBuffer(String prefix, Map<String, Object> metrics, long timestamp) {
    if (adapter != null) {
      adapter.appendToBuffer(prefix, metrics, timestamp);
    }
  }

  protected void emptyBuffer() {
    adapter.emptyBuffer();
  }

  protected void sendMetrics() {
    try {
      if (adapter != null) {
        adapter.sendBufferContents();
      }
    }
    catch (IOException e) {
      String trace = Throwables.getStackTraceAsString(e);
      String msg = "Could not send metrics update to backend server " + adapter.getServerFingerprint() + ": " + trace +
          " (" + adapter.getFailures() + " failed attempts so far)";
      LOG.error(msg);
    }
  }

  protected void graphiteDisconnect() {
    if (adapter != null) {
      try {
        adapter.disconnect();
      }
      catch (GraphiteConnectionFailureException e) {
        String trace = Throwables.getStackTraceAsString(e);
        LOG.error("Could not disconnect from adapter backend " + adapter.getServerFingerprint() + ": " + trace);
      }
    }
  }

  @Override
  public void cleanup() {
    graphiteDisconnect();
  }

}
