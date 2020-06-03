package com.linkedin.avro.fastserde;

public class FastSerdeGeneratorException extends RuntimeException {

  public FastSerdeGeneratorException(String message, Throwable cause) {
    super(message, cause);
  }

  public FastSerdeGeneratorException(String message) {
    super(message);
  }

  public FastSerdeGeneratorException(Throwable cause) {
    super(cause);
  }
}
