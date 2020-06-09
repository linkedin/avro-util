package com.linkedin.avro.fastserde;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.StringUtils;


/**
 * Utilities used by both serialization and deserialization code.
 *
 * TODO: Make {@link FastDeserializerGeneratorBase} and {@link FastSerializerGeneratorBase} more lightweight
 *       and less redundant by moving more stuff here.
 */
public abstract class FastSerdeBase {
  /**
   * A repository of how many times a given name was used.
   *
   * Does not actually need to be threadsafe, but it is made so just for defensive coding reasons.
   */
  private final ConcurrentMap<String, AtomicInteger> COUNTER_PER_NAME = new FastAvroConcurrentHashMap<>();

  /**
   * A function to generate unique names, such as those of variables and functions, within the scope
   * of the this class instance (i.e. per serializer of a given schema or deserializer of a given
   * schema pair).
   *
   * @param prefix String to serve as a prefix for the unique name
   * @return a unique prefix composed of the {@param prefix} appended by a unique number
   */
  protected String getUniqueName(String prefix) {
    String uncapitalizedPrefix = StringUtils.uncapitalize(prefix);
    return uncapitalizedPrefix + nextUniqueInt(uncapitalizedPrefix);
  }

  private int nextUniqueInt(String name) {
    return COUNTER_PER_NAME.computeIfAbsent(name, k -> new AtomicInteger(0)).getAndIncrement();
  }
}
