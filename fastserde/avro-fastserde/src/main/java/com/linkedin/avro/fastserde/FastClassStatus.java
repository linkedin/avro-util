package com.linkedin.avro.fastserde;

public interface FastClassStatus {
  /**
   * Whether it is implemented by the runtime-generated class or not.
   */
  default boolean isBackedByGeneratedClass() {
    return true;
  }

  /**
   * Tell the user, specifically {@link FastGenericDatumReader}/{@link FastSpecificDatumReader} or
   * {@link FastGenericDatumWriter}/{@link FastSpecificDatumWriter} that
   * whether runtime class generation is done or not.
   * There will be two outcomes:
   * 1. Fast class is backed by the class generated at runtime.
   * 2. Vanilla Avro based implementation when runtime class generation fails.
   *
   * For both cases, {@link FastGenericDatumReader}/{@link FastSpecificDatumReader} or
   * {@link FastGenericDatumWriter}/{@link FastSpecificDatumWriter}can stop polling
   * the runtime class generation status.
   */
  default boolean hasDynamicClassGenerationDone() {
    return true;
  }

}
