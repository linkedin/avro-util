package com.linkedin.avro.fastserde;

public class SchemaAssistantException extends RuntimeException {

  public SchemaAssistantException(String message, Throwable cause) {
    super(message, cause);
  }

  public SchemaAssistantException(String message) {
    super(message);
  }

  public SchemaAssistantException(Throwable cause) {
    super(cause);
  }
}
