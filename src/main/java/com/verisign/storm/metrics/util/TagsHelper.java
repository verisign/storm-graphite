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

import org.apache.storm.metric.api.IMetricsConsumer;
import com.google.common.base.Optional;

import java.util.HashMap;
import java.util.Map;

public class TagsHelper {
  public static final String GRAPHITE_PREFIX_OPTION = "metrics.graphite.prefix";
  public static final String DEFAULT_PREFIX = "metrics";

  public static String getPrefix(Map<String, Object> conf) {
    return (String) Optional.fromNullable(conf.get(GRAPHITE_PREFIX_OPTION)).or(DEFAULT_PREFIX);
  }

  public static Map<String, String> convertToTags(String stormId, IMetricsConsumer.TaskInfo taskInfo) {
    Map<String, String> tags = new HashMap<>();
    tags.put("srcComponentId", taskInfo.srcComponentId);
    tags.put("stormId", removeNonce(stormId));
    tags.put("rawStormId", stormId);
    tags.put("srcWorkerHost", taskInfo.srcWorkerHost);
    tags.put("srcWorkerPort", String.valueOf(taskInfo.srcWorkerPort));
    tags.put("srcTaskId", String.valueOf(taskInfo.srcTaskId));
    return tags;
  }

  /**
   * Constructs a fully qualified metric prefix.
   *
   * @param tags The information regarding the context in which the data point is supplied
   * @return A fully qualified metric prefix.
   */
  public static String constructMetricPrefix(String prefix, Map<String, String> tags) {
    StringBuilder sb = new StringBuilder();

    if (prefix == null) {
      throw new IllegalArgumentException("Prefix is required");
    }
    appendIfNotNullOrEmpty(sb, prefix);
    appendIfNotNullOrEmpty(sb, tags, "stormId");
    appendIfNotNullOrEmpty(sb, tags, "srcComponentId");
    appendIfNotNullOrEmpty(sb, tags, "srcWorkerHost");
    appendIfNotNullOrEmpty(sb, tags, "srcWorkerPort");
    appendIfNotNullOrEmpty(sb, tags, "srcTaskId");

    return sb.substring(0, sb.length() - 1);
  }

  /**
   * Removes nonce appended to topology name (e.g. "Example-Topology-1-2345" -> "Example-Topology")
   */
  private static String removeNonce(String topologyId) {
    return topologyId.substring(0, topologyId.substring(0, topologyId.lastIndexOf("-")).lastIndexOf("-"));
  }

  private static void appendIfNotNullOrEmpty(StringBuilder sb, Map<String, String> tags, String key) {
    String value = tags.get(key);
    appendIfNotNullOrEmpty(sb, value);
  }

  private static void appendIfNotNullOrEmpty(StringBuilder sb, String value) {
    if (value != null && !value.isEmpty()) {
      sb.append(value).append(".");
    }
  }
}
