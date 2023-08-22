package com.linkedin.avro.fastserde.logical.types;

import java.io.IOException;

@FunctionalInterface
public interface FunctionThrowingIOException<T, R> {

    R apply(T input) throws IOException;
}
