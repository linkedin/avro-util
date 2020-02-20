/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public enum AvroVersion {
  //MUST BE ORDERED IN INCREASING ORDER
  AVRO_1_4, AVRO_1_5, AVRO_1_6, AVRO_1_7, AVRO_1_8, AVRO_1_9;

  private final static AvroVersion EARLIEST;
  private final static AvroVersion LATEST;

  static {
    AvroVersion[] all = values();
    EARLIEST = all[0];
    LATEST = all[all.length - 1];
  }

  public static AvroVersion earliest() {
    return EARLIEST;
  }

  public static AvroVersion latest() {
    return LATEST;
  }

  public static AvroVersion fromSemanticVersion(String semVer) {
    Pattern versionPattern = Pattern.compile("(\\d+)\\.(\\d+)");
    Matcher matcher = versionPattern.matcher(semVer);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("unable to parse avro version out of " + semVer);
    }
    int major = Integer.parseInt(matcher.group(1));
    int minor = Integer.parseInt(matcher.group(2));
    return valueOf("AVRO_" + major + "_" + minor);
  }

  public boolean earlierThan(AvroVersion other) {
    return this.compareTo(other) < 0;
  }

  public boolean laterThan(AvroVersion other) {
    return this.compareTo(other) > 0;
  }
}
