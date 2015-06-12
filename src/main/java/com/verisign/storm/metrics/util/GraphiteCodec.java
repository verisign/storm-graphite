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

import java.util.Locale;

/**
 * Formats Java objects into Graphite-compatible String representations.
 */
//
// NOTICE:
//
// The code of the `format()` methods is taken from {@link com.codahale.metrics.graphite.GraphiteReporter}.  See the
// top-level `NOTICE` file for attribution.  Preferably, we would have simply used the existing code in Coda Hale's
// `GraphiteReporter` as-is, but the relevant `format()` methods are marked as private.
//
public class GraphiteCodec {

  public static String format(Object o) {
    if (o instanceof Float) {
      return format(((Float) o).doubleValue());
    }
    else if (o instanceof Double) {
      return format(((Double) o).doubleValue());
    }
    else if (o instanceof Byte) {
      return format(((Byte) o).longValue());
    }
    else if (o instanceof Short) {
      return format(((Short) o).longValue());
    }
    else if (o instanceof Integer) {
      return format(((Integer) o).longValue());
    }
    else if (o instanceof Long) {
      return format(((Long) o).longValue());
    }
    else if (o instanceof String) {
      return format(Double.parseDouble((String) o));
    }
    return null;
  }

  public static String format(long n) {
    return Long.toString(n);
  }

  public static String format(double d) {
    // the Carbon plaintext format is pretty underspecified, but it seems like it just wants
    // US-formatted digits
    return String.format(Locale.US, "%2.2f", d);
  }

}