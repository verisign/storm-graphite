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
import com.verisign.storm.metrics.reporters.AbstractReporter;
import com.verisign.storm.metrics.reporters.graphite.GraphiteReporter;
import com.verisign.storm.metrics.util.ConnectionFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Listens for all of Storm's built-in metrics and forwards them to a Graphite server.
 *
 * To use, add this to your topology's configuration:
 *
 * To report to a Graphite endpoint:
 *
 * <pre>
 * {@code
 *   conf.registerMetricsConsumer(com.verisign.storm.metrics.GraphiteMetricsConsumer.class, 1);
 *   conf.put("metrics.reporter.name", "com.verisign.storm.metrics.reporters.graphite.GraphiteReporter");
 *   conf.put("metrics.graphite.host", "<GRAPHITE HOSTNAME>");
 *   conf.put("metrics.graphite.port", "<GRAPHITE PORT>");
 *   conf.put("metrics.graphite.prefix", "<DOT DELIMITED PREFIX>");
 *   // Optional settings
 *   conf.put("metrics.graphite.min-connect-attempt-interval-secs", "5");
 * }
 * </pre>
 *
 * To report Avro-encoded metrics to a Kafka endpoint:
 * <pre>
 * {@code
 *   conf.registerMetricsConsumer(com.verisign.storm.metrics.GraphiteMetricsConsumer.class, 1);
 *   conf.put("metrics.reporter.name", "com.verisign.storm.metrics.reporters.kafka.AvroKafkaReporter");
 *   conf.put("metrics.graphite.prefix", "storm.cluster.metrics");
 *   conf.put("metrics.kafka.topic", "graphingMetrics");
 *   conf.put("metrics.kafka.metadata.broker.list", "kafka1.example.com:9092,kafka2.example.com:9092");
 * }
 * </pre>
 *
 * To report Avro-encoded metrics to a Confluent Schema-Registry enabled Kafka endpoint:
 * <pre>
 * {@code
 *   conf.registerMetricsConsumer(com.verisign.storm.metrics.GraphiteMetricsConsumer.class, 1);
 *   conf.put("metrics.reporter.name", "com.verisign.storm.metrics.reporters.kafka.SchemaRegistryKafkaReporter");
 *   conf.put("metrics.graphite.prefix", "storm.cluster.metrics");
 *   conf.put("metrics.kafka.topic", "graphingMetrics");
 *   conf.put("metrics.kafka.metadata.broker.list", "kafka1.example.com:9092,kafka2.example.com:9092");
 *   conf.put("metrics.kafka.schema.registry.url", "http://schemaregistry.example.com:8081");
 * }
 * </pre>   
 *
 *
 * Or edit the storm.yaml config file:
 *
 * To report to a Graphite endpoint: 
 * <pre>
 * {@code
 *   topology.metrics.consumer.register:
 *     - class: "com.verisign.storm.metrics.GraphiteMetricsConsumer"
 *       parallelism.hint: 1
 *       argument:
 *         metrics.reporter.name: "com.verisign.storm.metrics.reporters.graphite.GraphiteReporter"
 *         metrics.graphite.host: "<GRAPHITE HOSTNAME>"
 *         metrics.graphite.port: "<GRAPHITE PORT>"
 *         metrics.graphite.prefix: "<DOT DELIMITED PREFIX>"
 *         metrics.graphite.min-connect-attempt-interval-secs: "5"
 * }
 * </pre>
 *
 * To report Avro-encoded metrics to a Kafka endpoint:
 * <pre>
 * {@code
 *  topology.metrics.consumer.register:
 *    - class: "com.verisign.storm.metrics.GraphiteMetricsConsumer"
 *      parallelism.hint: 1
 *      argument:
 *        metrics.reporter.name: "com.verisign.storm.metrics.reporters.kafka.AvroKafkaReporter"
 *        metrics.graphite.prefix: "storm.cluster.metrics"
 *        metrics.kafka.topic: "graphingMetrics"
 *        metrics.kafka.metadata.broker.list: "kafka1.example.com:9092,kafka2.example.com:9092"
 * }
 * </pre> 
 *
 * To report Avro-encoded metrics to a Confluent Schema-Registry enabled Kafka endpoint:
 * <pre>
 * {@code
 *  topology.metrics.consumer.register:
 *    - class: "com.verisign.storm.metrics.GraphiteMetricsConsumer"
 *      parallelism.hint: 1
 *      argument:
 *        metrics.reporter.name: "com.verisign.storm.metrics.reporters.kafka.SchemaRegistryKafkaReporter"
 *        metrics.graphite.prefix: "storm.cluster.metrics"
 *        metrics.kafka.topic: "graphingMetrics"
 *        metrics.kafka.metadata.broker.list: "kafka1.example.com:9092,kafka2.example.com:9092"
 *        metrics.kafka.schema.registry.url: "http://schemaregistry.example.com:8081"
 * }
 * </pre>
 */
public class GraphiteMetricsConsumer implements IMetricsConsumer {

  public static final String GRAPHITE_PREFIX_OPTION = "metrics.graphite.prefix";
  public static final String REPORTER_NAME = "metrics.reporter.name";
  private static final Logger LOG = LoggerFactory.getLogger(GraphiteMetricsConsumer.class);
  private static final String DEFAULT_PREFIX = "metrics";
  private static final String DEFAULT_REPORTER = GraphiteReporter.class.getCanonicalName();

  private String graphitePrefix;
  private String stormId;

  protected AbstractReporter reporter;

  protected String getStormId() {
    return stormId;
  }

  protected String getGraphitePrefix() {
    return graphitePrefix;
  }

  @Override
  public void prepare(Map config, Object registrationArgument, TopologyContext context, IErrorReporter errorReporter) {

    Map configMap = new HashMap<Object, Object>();
    if (config != null) {
      LOG.info("Configuration parameter: {}", config.toString());
      configMap.putAll(config);
    }
    else {
      LOG.warn("No reference to configuration parameter config found.");
    }

    if (registrationArgument != null && registrationArgument instanceof Map) {
      LOG.info("Registration argument: {}", registrationArgument.toString());
      configMap.putAll((Map) registrationArgument);
    }
    else {
      LOG.warn("No reference to configuration parameter registrationArgument found");
    }

    if (configMap.containsKey(GRAPHITE_PREFIX_OPTION)) {
      graphitePrefix = (String) configMap.get(GRAPHITE_PREFIX_OPTION);
    }
    else {
      graphitePrefix = DEFAULT_PREFIX;
    }

    if (!configMap.containsKey(REPORTER_NAME)) {
      configMap.put(REPORTER_NAME, DEFAULT_REPORTER);
    }

    try {
      reporter = configureReporter(configMap);
    }
    catch (Exception e) {
      LOG.error("Error configuring metrics reporter:" + e.getMessage());
      errorReporter.reportError(e);
    }

    stormId = context.getStormId();
  }

  private AbstractReporter configureReporter(@SuppressWarnings("rawtypes") Map reporterConfig)
      throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException,
      InvocationTargetException {
    String className = (String) reporterConfig.get(REPORTER_NAME);
    Class reporterClass = Class.forName(className);
    AbstractReporter reporter = (AbstractReporter) reporterClass.newInstance();
    reporter.prepare(reporterConfig);

    return reporter;
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

      if (dataPoint.value == null) {
        LOG.warn("Data point with name {} has a value of null. Discarding data point", dataPoint.name);
      }
      else if (dataPoint.value instanceof Map) {
        Map<String, Object> dataMap = (Map<String, Object>) dataPoint.value;
        Map<String, Double> bufferMap = new HashMap<String, Double>();

        for (Map.Entry<String, Object> entry : dataMap.entrySet()) {
          Double dblValue = convertToDoubleValue(entry.getKey(), entry.getValue());
          if (dblValue != null) {
            bufferMap.put(dataPoint.name + "." + entry.getKey(), dblValue);
          }
        }

        appendToReporterBuffer(metricPrefix, bufferMap, taskInfo.timestamp);
      }
      else if (dataPoint.value instanceof Number) {
        Double dblValue = ((Number) dataPoint.value).doubleValue();
        HashMap<String, Double> metric = new HashMap<String, Double>();
        metric.put(dataPoint.name + "value", dblValue);
        appendToReporterBuffer(metricPrefix, metric, taskInfo.timestamp);
      }
      else {
        LOG.warn("Unrecognized metric value of type {} received. Discarding metric.",
            dataPoint.value.getClass().getName());
      }
    }
    sendMetrics();
    graphiteDisconnect();
  }

  private Double convertToDoubleValue(String key, Object value) {
    if (value != null) {
      Double result;

      if (value instanceof String) {
        try {
          result = Double.parseDouble((String) value);
        }
        catch (NumberFormatException e) {
          LOG.warn("Failed to parse metric with key {} and value of {} into a Double. Discarding metric.", key, value);
          result = null;
        }
      }
      else if (value instanceof Number) {
        result = ((Number) value).doubleValue();
      }
      else {
        result = null;
      }

      return result;
    }
    else {
      LOG.warn("Metric with key {} has a value of null. Discarding metric.", key);
      return null;
    }
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
    sb.append(taskInfo.srcTaskId);
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
      reporter.connect();
    }
    catch (ConnectionFailureException e) {
      String trace = Throwables.getStackTraceAsString(e);
      LOG.error("Could not connect to reporter backend " + reporter.getBackendFingerprint() + ": " + trace);
    }
  }

  protected void appendToReporterBuffer(String prefix, Map<String, Double> metrics, long timestamp) {
    if (reporter != null) {
      if (!metrics.isEmpty()) {
        reporter.appendToBuffer(prefix, metrics, timestamp);
      }
      else {
        LOG.warn("Dropping metrics map with prefix {} because it is empty.", prefix);
      }
    }
    else {
      LOG.error("Failed to append metrics to buffer because backend reporter is undefined.");
    }
  }

  protected void emptyBuffer() {
    reporter.emptyBuffer();
  }

  protected void sendMetrics() {
    try {
      if (reporter != null) {
        reporter.sendBufferContents();
      }
      else {
        LOG.error("Failed to send metrics because backend reporter is undefined.");
      }
    }
    catch (IOException e) {
      String trace = Throwables.getStackTraceAsString(e);
      String msg = "Could not send metrics update to backend server " + reporter.getBackendFingerprint() + ": " + trace
          +
          " (" + reporter.getFailures() + " failed attempts so far)";
      LOG.error(msg);
    }
  }

  protected void graphiteDisconnect() {
    if (reporter != null) {
      try {
        reporter.disconnect();
      }
      catch (ConnectionFailureException e) {
        String trace = Throwables.getStackTraceAsString(e);
        LOG.error("Could not disconnect from reporter backend " + reporter.getBackendFingerprint() + ": " + trace);
      }
    }
  }

  @Override
  public void cleanup() {
    graphiteDisconnect();
  }

}
