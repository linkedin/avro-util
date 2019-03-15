/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avro.legacy;

import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class PayloadFindAndReplaceStep implements PayloadTransformStep {
  private final Map<String, String> replacements;
  private final Pattern pattern;

  public PayloadFindAndReplaceStep(Map<String, String> replacements) {
    this.replacements = replacements;
    StringJoiner orJoiner = new StringJoiner("|");
    replacements.forEach((from, to) -> {
      orJoiner.add(Pattern.quote(from));
    });
    this.pattern = Pattern.compile(String.format("\"%s\"", orJoiner));
  }

  @Override
  public String applyToJsonPayload(String jsonPayload) {
    Matcher matcher = pattern.matcher(jsonPayload);
    StringBuffer sb = new StringBuffer();
    while (matcher.find()) {
      String found = matcher.group();
      found = found.substring(1, found.length() - 1); //strip enclosing quotes
      String replacement = replacements.get(found);
      if (replacement == null) {
        throw new IllegalStateException(); //should never happen
      }
      matcher.appendReplacement(sb, String.format("\"%s\"", replacement));
    }
    matcher.appendTail(sb);
    return sb.toString();
  }

  @Override
  public PayloadTransformStep inverse() {
    Map<String, String> inverseMap = new HashMap<>();
    replacements.forEach((k, v) -> {
      if (inverseMap.put(v, k) != null) {
        throw new IllegalStateException("cannot reverse non-unique mapping");
      }
    });
    return new PayloadFindAndReplaceStep(inverseMap);
  }

  @Override
  public String toString() {
    StringJoiner csvJoiner = new StringJoiner(", ");
    replacements.forEach((from, to) -> {
      csvJoiner.add(from + "-->" + to);
    });
    return String.format("{%s}", csvJoiner);
  }
}
