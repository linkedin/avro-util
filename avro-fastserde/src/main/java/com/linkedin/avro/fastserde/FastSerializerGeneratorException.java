package com.linkedin.avro.fastserde;

public class FastSerializerGeneratorException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public FastSerializerGeneratorException(String message, Throwable cause) {
    super(message, cause);
  }

  public FastSerializerGeneratorException(String message) {
    super(message);
  }

  public FastSerializerGeneratorException(Throwable cause) {
    super(cause);
  }
}
