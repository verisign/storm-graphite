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

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.HashMap;

import static org.fest.assertions.api.Assertions.assertThat;

public class GraphiteCodecTest {

  @DataProvider(name = "generateFloats")
  private Object[][] generateFloats() {
    return new Object[][] { new Object[] { new Float(0.0), "0.00" },
        new Object[] { Float.MAX_VALUE, "340282346638528860000000000000000000000.00" },
        new Object[] { new Float(3.1415926), "3.14" }, new Object[] { new Float(2.71828), "2.72" } };
  }

  @Test(dataProvider = "generateFloats")
  public void readFloatAndReturnFormattedString(Float data, String expectedOutput) {
    String actualOutput = GraphiteCodec.format(data);
    assertThat(actualOutput).isEqualTo(expectedOutput);
  }

  @DataProvider(name = "generateDoubles")
  private Object[][] generateDoubles() {
    return new Object[][] { new Object[] { Double.MAX_VALUE,
        "179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.00" },
        new Object[] { Math.PI, "3.14" }, new Object[] { Math.E, "2.72" }, new Object[] { 0.0, "0.00" } };
  }

  @Test(dataProvider = "generateDoubles")
  public void readDoubleAndReturnFormattedString(Double data, String expectedOutput) {
    String actualOutput = GraphiteCodec.format(data);
    assertThat(actualOutput).isEqualTo(expectedOutput);
  }

  @DataProvider(name = "generateBytes")
  private Object[][] generateBytes() {
    return new Object[][] { new Object[] { Byte.MAX_VALUE, "127" }, new Object[] { Byte.MIN_VALUE, "-128" },
        new Object[] { new Byte("0"), "0" }, new Object[] { new Byte("1"), "1" },
        new Object[] { new Byte("-1"), "-1" } };
  }

  @Test(groups = { "checkin", "unit" },
      dataProvider = "generateBytes")
  public void readByteAndReturnFormattedString(Byte data, String expectedOutput) {
    String actualOutput = GraphiteCodec.format(data);
    assertThat(actualOutput).isEqualTo(expectedOutput);
  }

  @DataProvider(name = "generateShorts")
  private Object[][] generateShorts() {
    return new Object[][] { new Object[] { Short.MAX_VALUE, "32767" }, new Object[] { Short.MIN_VALUE, "-32768" },
        new Object[] { new Short("0"), "0" }, new Object[] { new Short("-1"), "-1" },
        new Object[] { new Short("1"), "1" } };
  }

  @Test(dataProvider = "generateShorts")
  public void readShortAndReturnFormattedString(Short data, String expectedOutput) {
    String actualOutput = GraphiteCodec.format(data);
    assertThat(actualOutput).isEqualTo(expectedOutput);
  }

  @DataProvider(name = "generateInts")
  private Object[][] generateInts() {
    return new Object[][] { new Object[] { Integer.MAX_VALUE, "2147483647" },
        new Object[] { Integer.MIN_VALUE, "-2147483648" }, new Object[] { new Integer(0), "0" },
        new Object[] { new Integer(1), "1" }, new Object[] { new Integer(-1), "-1" } };
  }

  @Test(dataProvider = "generateInts")
  public void readIntAndReturnFormattedString(Integer data, String expectedOutput) {
    String actualOutput = GraphiteCodec.format(data);
    assertThat(actualOutput).isEqualTo(expectedOutput);
  }

  @DataProvider(name = "generateLongs")
  private Object[][] generateLongs() {
    return new Object[][] { new Object[] { Long.MAX_VALUE, "9223372036854775807" },
        new Object[] { Long.MIN_VALUE, "-9223372036854775808" }, new Object[] { new Long("0"), "0" },
        new Object[] { new Long("-1"), "-1" }, new Object[] { new Long("1"), "1" } };
  }

  @Test(dataProvider = "generateLongs")
  public void readLongAndReturnFormattedString(Long data, String expectedOutput) {
    String actualOutput = GraphiteCodec.format(data);
    assertThat(actualOutput).isEqualTo(expectedOutput);
  }

  @Test
  public void readInvalidClassAndReturnNull() {
    String actualOutput = GraphiteCodec.format(new HashMap<String, Object>());
    assertThat(actualOutput).isNull();
  }
}
