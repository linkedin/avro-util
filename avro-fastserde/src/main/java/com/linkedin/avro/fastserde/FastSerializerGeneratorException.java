package com.linkedin.avro.fastserde;

public class FastSerializerGeneratorException extends RuntimeException {

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
