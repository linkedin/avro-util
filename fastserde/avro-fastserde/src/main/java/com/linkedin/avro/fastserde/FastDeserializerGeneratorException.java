package com.linkedin.avro.fastserde;

/** TODO Replace all usages by {@link FastSerdeGeneratorException} */
public class FastDeserializerGeneratorException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public FastDeserializerGeneratorException(String message, Throwable cause) {
    super(message, cause);
  }

  public FastDeserializerGeneratorException(String message) {
    super(message);
  }

  public FastDeserializerGeneratorException(Throwable cause) {
    super(cause);
  }
}
