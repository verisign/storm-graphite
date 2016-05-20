package com.verisign.storm.metrics.util;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Simple token replacement utility class.
 */
public class TokenReplacement {
  private static final Pattern tokenPattern = Pattern.compile("%%([^%]+)%%");

  /**
   * Replaces variable tags like:
   * %%TOPOLOGY%% or  %%COMPONENT%% %%WHATEVERYOUWANT%%
   * @param sourceString - the source string
   * @param replacements - map of token => replacement value
   * @return modified string.
   */
  public static String replaceTokens(String sourceString, Map<String, String> replacements) {
    Matcher matcher = tokenPattern.matcher(sourceString);
    StringBuffer buffer = new StringBuffer();
    while (matcher.find()) {
      String replacement = replacements.get(matcher.group(1));
      if (replacement != null) {
        matcher.appendReplacement(buffer, "");
        buffer.append(replacement);
      }
    }
    matcher.appendTail(buffer);
    return buffer.toString();
  }
}
