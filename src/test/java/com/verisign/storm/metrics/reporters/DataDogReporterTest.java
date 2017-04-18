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

import com.timgroup.statsd.StatsDClient;
import com.verisign.storm.metrics.reporters.datadog.DataDogReporter;
import com.verisign.storm.metrics.util.TagsHelper;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class DataDogReporterTest {
  private DataDogReporter reporter = new DataDogReporter();
  private StatsDClient statsDClient = null;
  private static String topologyName = "foo";

  private void initMockReporter(){
    Map<String, Object> reporterConfig = new HashMap<String, Object>();
    reporterConfig.put(TagsHelper.GRAPHITE_PREFIX_OPTION, ReporterDataProvider.testPrefix);
    reporterConfig.put("topology.name", "defaulttopologyname");
    reporterConfig.put(DataDogReporter.TOPOLOGYNAME, topologyName);

    statsDClient = mock(StatsDClient.class);
    reporter.prepare(reporterConfig, statsDClient);
  }

  @Test(dataProvider = "dogMetrics")
  public void sendMetricUsingDogReporter(HashMap<String,String> tags, String metricType, String metricKey, Double value,
      long timestamp) throws IOException {
    initMockReporter();

    HashMap<String, Double> values = new HashMap<String, Double>();
    values.put(metricKey, value);
    reporter.appendToBuffer(tags, values, timestamp);

    if (metricType == "latency"){
      verify(statsDClient).recordExecutionTime(constructDogPrefix(tags, metricKey), Double.valueOf(value * 1000d).longValue(), buildDogTags(tags));
    }
    else{
      verify(statsDClient).gauge(constructDogPrefix(tags, metricKey), value, buildDogTags(tags));
    }
  }

  private String[] buildDogTags(Map<String, String> tags) {
    return new String[] {
      "srcWorkerPort:" + tags.get("srcWorkerPort"),
      "srcTaskId:" + tags.get("srcTaskId"),
      "srcWorkerHost:" + tags.get("srcWorkerHost")
    };
  }

  private String constructDogPrefix(Map<String, String> tags, String metric){
    return String.format("%s.%s.%s", topologyName, tags.get("srcComponentId").toLowerCase(), metric);
  }

  @DataProvider(name = "dogMetrics")
  public Object[][] dogMetricProvider() {
    return new ReporterDataProvider().dogMetricProvider();
  }
}
