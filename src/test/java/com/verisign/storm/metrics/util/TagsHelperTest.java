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

package com.verisign.storm.metrics.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.storm.metric.api.IMetricsConsumer;

import org.testng.annotations.DataProvider;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.Test;

import static org.fest.assertions.api.Assertions.assertThat;

public class TagsHelperTest {
  private final String stormId = "Example-Topology";
  private final String rawStormId = String.format("%s-1-2345", stormId);

  @DataProvider(name = "taskInfo")
  public Object[][] generateTaskInfo() {
    return new Object[][]{
      generateTaskInfo(stormId, rawStormId),
      generateTaskInfo(stormId, rawStormId)
    };
  }

  @DataProvider(name = "prefix")
  public Object[][] generatePrefix() {
    String prefixConfig = RandomStringUtils.randomAlphanumeric(10);
    HashMap<String, String> tags1 = generateTags(stormId, rawStormId);
    HashMap<String, String> tags2 = generateTags(stormId, rawStormId);
    return new Object[][]{
      new Object[]{prefixConfig, tags1, getPrefix(prefixConfig, tags1)},
      new Object[]{prefixConfig, tags2, getPrefix(prefixConfig, tags2)}
    };
  }

  @Test(dataProvider = "taskInfo")
  public void convertToTags(String stormId, IMetricsConsumer.TaskInfo taskInfo, Map<String, String> expectedTags) {
    assertThat(TagsHelper.convertToTags(stormId, taskInfo)).isEqualTo(expectedTags);
  }

  @Test(dataProvider = "prefix")
  public void constructPrefix(String prefixConfig, Map<String, String> tags, String expectedPrefix) {
    assertThat(TagsHelper.constructMetricPrefix(prefixConfig, tags)).isEqualTo(expectedPrefix);
  }

  private Object[] generateTaskInfo(String stormId, String rawStormId) {
    HashMap<String, String> tags = generateTags(stormId, rawStormId);
    return new Object[]{
      rawStormId,
      new IMetricsConsumer.TaskInfo(
        tags.get("srcWorkerHost"),
        Integer.valueOf(tags.get("srcWorkerPort")),
        tags.get("srcComponentId"),
        Integer.valueOf(tags.get("srcTaskId")),
        System.currentTimeMillis(),
        10),
      tags
    };
  }

  private String getPrefix(String prefixConfig, Map<String, String> tags) {
    return String.format("%s.%s.%s.%s.%s.%s",
      prefixConfig,
      tags.get("stormId"),
      tags.get("srcComponentId"),
      tags.get("srcWorkerHost"),
      tags.get("srcWorkerPort"),
      tags.get("srcTaskId")
    );
  }

  private HashMap<String, String> generateTags(String stormId, String rawStormId) {
    String testStormComponentID = RandomStringUtils.randomAlphanumeric(20);
    String testStormSrcWorkerHost = RandomStringUtils.randomAlphanumeric(20);
    Integer testStormSrcWorkerPort = 6700;
    Integer testStormSrcTaskId = 3008;

    HashMap<String, String> expectedTags = new HashMap<String, String>();
    expectedTags.put("rawStormId", rawStormId);
    expectedTags.put("stormId", stormId);
    expectedTags.put("srcComponentId", testStormComponentID);
    expectedTags.put("srcWorkerHost", testStormSrcWorkerHost);
    expectedTags.put("srcWorkerPort", String.valueOf(testStormSrcWorkerPort));
    expectedTags.put("srcTaskId", String.valueOf(testStormSrcTaskId));
    return expectedTags;
  }
}
