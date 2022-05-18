package com.linkedin.avroutil1.parser;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.avro.Schema.Type;


public class HelperConsts {
  public static final Set<Type> PRIMITIVE_TYPES;
  public static final Set<Type> NAMED_TYPES;

  private HelperConsts() {
  }

  static {
    PRIMITIVE_TYPES = Collections.unmodifiableSet(new HashSet(
        Arrays.asList(Type.NULL, Type.BOOLEAN, Type.INT, Type.LONG, Type.FLOAT, Type.DOUBLE, Type.BYTES, Type.STRING)));
    NAMED_TYPES = Collections.unmodifiableSet(new HashSet(Arrays.asList(Type.RECORD, Type.ENUM, Type.FIXED)));
  }
}
