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

import org.testng.annotations.DataProvider;
import org.apache.commons.lang3.RandomStringUtils;
import com.verisign.storm.metrics.util.TagsHelper;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.*;

public class ReporterDataProvider {

  public static String testPrefix = "testPrefix";

  private Map<String, String> buildTags1() {
    return buildTags(new String[]{
      "srcComponentId", "stormId", "srcWorkerHost", "srcWorkerPort", "srcTaskId"
    });
  }

  private Map<String, String> buildTags2() {
    return buildTags(new String[]{
      "srcComponentId", "srcWorkerPort", "srcTaskId"
    });
  }

  public Object[][] metricsProvider() {
    Map<String, String> tags1 = buildTags1();
    Map<String, String> tags2 = buildTags2();

    String tags1Prefix = TagsHelper.constructMetricPrefix(testPrefix, tags1);
    String tags2Prefix = TagsHelper.constructMetricPrefix(testPrefix, tags2);

    return new Object[][]{
      new Object[]{tags1, "metric1", 1.00, new Long("1408393534971"),
        String.format("%s.metric1 1.00 1408393534971\n", tags1Prefix)},
      new Object[]{tags1, "metric2", 0.00, new Long("1408393534971"),
        String.format("%s.metric2 0.00 1408393534971\n", tags1Prefix)},
      new Object[]{tags1, "metric3", 3.14, new Long("1408393534971"),
        String.format("%s.metric3 3.14 1408393534971\n", tags1Prefix)},
      new Object[]{tags2, "metric3", 99.0, new Long("1408393534971"),
        String.format("%s.metric3 99.00 1408393534971\n", tags2Prefix)},
      new Object[]{tags2, "metric3", 1e3, new Long("1408393534971"),
        String.format("%s.metric3 1000.00 1408393534971\n", tags2Prefix)}
    };
  }

  public Object[][] kafkaMetricsProvider() {
    int testCount = 10;
    Random rng = new Random(System.currentTimeMillis());
    Object[][] testData = new Object[testCount][];

    for (int i = 0; i < testCount; i++) {
      List<Object> data = new ArrayList<Object>();

      data.add(buildTags1());

      String metricName = new BigInteger(50, rng).toString(32);
      data.add(metricName);

      Double metricValue = rng.nextDouble();
      data.add(metricValue);

      Double truncatedValue = Double.parseDouble(String.format("%2.2f", metricValue));
      data.add(truncatedValue);

      Long timestamp = System.currentTimeMillis();
      data.add(timestamp);

      testData[i] = data.toArray();
    }

    return testData;
  }

  public Object[][] dogMetricProvider() {
    Map<String, String> tags1 = buildTags1();

    return new Object[][]{
      new Object[]{tags1, "latency", "latencymetric1", 0.002, new Long("1408393534971")},
      new Object[]{tags1, "guage", "metric2", 0.00, new Long("1408393534971")},
      new Object[]{tags1, "latency", "latencymetric3", 3.14, new Long("1408393534971")},
      new Object[]{tags1, "unknown", "metric3", 99.0, new Long("1408393534971")},
      new Object[]{tags1, "guage", "metric4", 1e3, new Long("1408393534971")}
    };
  }

  private Map<String, String> buildTags(String[] keys) {
    HashMap<String, String> tags = new HashMap<String, String>();
    for (String key : keys) {
      tags.put(key, RandomStringUtils.randomAlphanumeric(10));
    }
    return tags;
  }
}
