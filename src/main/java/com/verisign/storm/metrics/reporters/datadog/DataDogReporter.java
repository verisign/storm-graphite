package com.verisign.storm.metrics.reporters.datadog;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import com.verisign.storm.metrics.reporters.AbstractReporter;
import com.verisign.storm.metrics.util.ConnectionFailureException;
import com.verisign.storm.metrics.util.TagsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DataDogReporter  extends AbstractReporter{
  private static final Logger logger = LoggerFactory.getLogger(DataDogReporter.class.getName());

  public static final String HOST = "metrics.datadog.host";
  public static final String PORT = "metrics.datadog.port";
  public static final String TAGS = "metrics.datadog.globaltags";
  public static final String TOPOLOGYNAME = "metrics.datadog.topologyname";

  private ImmutableList<String> tagsKey = ImmutableList.of("srcWorkerPort", "srcTaskId", "srcWorkerHost");
  private StatsDClient statsd;
  private String topologyName;

  @Override
  public void prepare(Map<String, Object> conf) {
    prepare(conf, null);
  }

  /**
  * This method makes it easy to pass statsd instance during the tests
  */
  public void prepare(Map<String, Object> conf, StatsDClient statsd){
    topologyName = (String) Optional.fromNullable(conf.get(TOPOLOGYNAME)).or(getRequiredValue(conf, "topology.name"));

    if (statsd == null) {
      String statsdHost = (String) Optional.fromNullable(conf.get(HOST)).or("localhost");
      Integer port = (Integer) Optional.fromNullable(conf.get(PORT)).or(8125);
      String[] globalTags = (String[]) Optional.fromNullable(conf.get(TAGS)).or(new String[0]);

      logger.info("Initializing the datadog statsd client for {} with value {}:{} {}", topologyName, statsdHost, port, globalTags);
      this.statsd = new NonBlockingStatsDClient(
        TagsHelper.getPrefix(conf),
        statsdHost,
        port,
        globalTags
      );
    }
    else {
      this.statsd = statsd;
    }
  }

  @Override
  public void connect() throws ConnectionFailureException {
    //noop
  }

  @Override
  public void disconnect() throws ConnectionFailureException {
    //noop
  }

  @Override
  public void appendToBuffer(Map<String, String> tags, Map<String, Double> metrics, long timestamp) {
    logger.debug(String.format("Sending metrics to datadog for %s.%s", tags.get("srcComponentId"), metrics.keySet().toString()));

    String[] dTags = transformTags(tags);

    for (Map.Entry<String, Double> metric : metrics.entrySet()) {
      String name = getPrefix(tags, metric.getKey());
      if (name.contains("latency")){
        // we are going to lose the decimal points when converting double to long; instead report latency in micros for
        // better precision
        statsd.recordExecutionTime(name, Double.valueOf(metric.getValue() * 1000d).longValue(), dTags);
      }else {
        //default to gauge
        statsd.gauge(name, metric.getValue(), dTags);
      }
    }
  }

  @Override
  public void emptyBuffer() {
    //noop
  }

  @Override
  public void sendBufferContents() throws IOException {
    //noop
  }

  @Override
  public long getFailures() {
    return 0;
  }

  @Override
  public String getBackendFingerprint() {
    return String.format("%s.%s", statsd.getClass(), topologyName);
  }

  private Object getRequiredValue(Map<String, Object> conf, String key){
    if(conf.containsKey(key)){
      return conf.get(key);
    }else {
      throw new IllegalArgumentException(String.format("Field %s is required", key));
    }
  }

  private String[] transformTags(Map<String, String> tags){
    List<String> tagsTemp = new ArrayList<String>();
    for(String tag : tagsKey) {
      String value = Optional.fromNullable(tags.get(tag)).or("");
      tagsTemp.add(tag + ":" + value);
    }
    return tagsTemp.toArray(new String[0]);
  }

  private String getPrefix(Map<String, String> tags, String metric){
    return String.format("%s.%s.%s", topologyName, tags.get("srcComponentId"), metric)
      .toLowerCase()
      .replace('-', '_')
      .replace(':','_');
  }

  public void cleanUp(){
    if (statsd != null) {
      statsd.stop();
    }
  }
}
