package com.linkedin.avro.fastserde;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;


public class FastAvroConcurrentHashMap<K, V> extends ConcurrentHashMap<K, V> {
  public FastAvroConcurrentHashMap() {
    super();
  }

  public FastAvroConcurrentHashMap(int initialCapacity) {
    super(initialCapacity);
  }

  /**
   * The native `computeIfAbsent` function implemented in Java could have contention when
   * the value already exists {@link ConcurrentHashMap#computeIfAbsent(Object, Function)};
   * the contention could become very bad when lots of threads are trying to "computeIfAbsent"
   * on the same key, which is a known Java bug: https://bugs.openjdk.java.net/browse/JDK-8161372
   *
   * This internal FastAvroConcurrentHashMap mitigate such contention by trying to get the
   * value first before invoking "computeIfAbsent", which brings great optimization if the major
   * workload is trying to get the same key over and over again; however, we speculate that this
   * optimization might not be ideal for the workload that most keys are unique, so be cautious
   * about the workload before adopting the FastAvroConcurrentHashMap.
   *
   * @param key key with which the specified value is to be associated
   * @param mappingFunction the function to compute a value
   * @return the current (existing or computed) value associated with
   *         the specified key, or null if the computed value is null
   * @throws NullPointerException if the specified key or mappingFunction
   *         is null
   * @throws IllegalStateException if the computation detectably
   *         attempts a recursive update to this map that would
   *         otherwise never complete
   * @throws RuntimeException or Error if the mappingFunction does so,
   *         in which case the mapping is left unestablished
   */
  @Override
  public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
    V value = get(key);
    if (value != null) {
      return value;
    }
    return super.computeIfAbsent(key, mappingFunction);
  }
}
