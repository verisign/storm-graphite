package com.verisign.storm.metrics.util;

import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.fest.assertions.api.Assertions.assertThat;

/**
 *
 */
public class TokenReplacementTest {

  /**
   * Simple single token replacement test.
   */
  @Test
  public void testSimpleReplacement() {
    final String sourceTest = "This is my test %%token_here%%.";
    final Map<String, String> tokens = new HashMap<String, String>();
    tokens.put("token_here", "replacement text");

    final String result = TokenReplacement.replaceTokens(sourceTest, tokens);
    assertThat(result).isEqualTo("This is my test replacement text.");
  }

  /**
   * Multi token replacement test.
   */
  @Test
  public void testMultiReplacement() {
    final String sourceTest = "%%token1%%%%token2%% and %%token3%%";
    final Map<String, String> tokens = new HashMap<String, String>();
    tokens.put("token1", "ABC");
    tokens.put("token2", "DEF");
    tokens.put("token3", "XYZ");

    final String result = TokenReplacement.replaceTokens(sourceTest, tokens);
    assertThat(result).isEqualTo("ABCDEF and XYZ");
  }

  /**
   * Multi token replacement test, where some tokens are repeated.
   */
  @Test
  public void testReplaceSameTokenMultipleTimes() {
    final String sourceTest = "%%token1%% == %%token1%% != %%token2%%";
    final Map<String, String> tokens = new HashMap<String, String>();
    tokens.put("token1", "ABC");
    tokens.put("token2", "DEF");

    final String result = TokenReplacement.replaceTokens(sourceTest, tokens);
    assertThat(result).isEqualTo("ABC == ABC != DEF");
  }
}
