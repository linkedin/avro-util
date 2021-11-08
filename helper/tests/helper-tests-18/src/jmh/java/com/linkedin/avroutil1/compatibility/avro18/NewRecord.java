/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro18;

import java.util.concurrent.TimeUnit;
import org.apache.avro.specific.SpecificRecord;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@Fork(value = 1, warmups = 0)
@Warmup(iterations = 1, time = 10, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(value = TimeUnit.MILLISECONDS)
public class NewRecord {
  private static final String MESSAGE = "Hello, World!";

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder().include(NewRecord.class.getSimpleName()).build();
    new Runner(opt).run();
  }

  private static SpecificRecord set(SpecificRecord record) {
    record.put(0, MESSAGE);
    return record;
  }

  @Benchmark
  public SpecificRecord baseline() {
    return null;
  }

  @Benchmark
  public SpecificRecord nativeVanilla14() {
    return set(new by14.SimpleRecord());
  }

  @Benchmark
  public SpecificRecord nativeVanilla15() {
    return set(new by15.SimpleRecord());
  }

  @Benchmark
  public SpecificRecord nativeVanilla16() {
    return set(new by16.SimpleRecord());
  }

  @Benchmark
  public SpecificRecord nativeVanilla17() {
    return set(new by17.SimpleRecord());
  }

  @Benchmark
  public SpecificRecord nativeVanilla18() {
    return set(new by18.SimpleRecord());
  }

  @Benchmark
  public SpecificRecord nativeVanilla19() {
    return set(new by19.SimpleRecord());
  }

  @Benchmark
  public SpecificRecord nativeVanilla110() {
    return set(new by110.SimpleRecord());
  }

  @Benchmark
  public SpecificRecord nativeVanilla111() {
    return set(new by111.SimpleRecord());
  }

  @Benchmark
  public SpecificRecord nativeBuilder16() {
    return by16.SimpleRecord.newBuilder().setStringField(MESSAGE).build();
  }

  @Benchmark
  public SpecificRecord nativeBuilder17() {
    return by17.SimpleRecord.newBuilder().setStringField(MESSAGE).build();
  }

  @Benchmark
  public SpecificRecord nativeBuilder18() {
    return by18.SimpleRecord.newBuilder().setStringField(MESSAGE).build();
  }

  @Benchmark
  public SpecificRecord compatVanilla14() {
    return set(new under14.SimpleRecord());
  }

  @Benchmark
  public SpecificRecord compatVanilla15() {
    return set(new under15.SimpleRecord());
  }

  @Benchmark
  public SpecificRecord compatVanilla16() {
    return set(new under16.SimpleRecord());
  }

  @Benchmark
  public SpecificRecord compatVanilla17() {
    return set(new under17.SimpleRecord());
  }

  @Benchmark
  public SpecificRecord compatVanilla18() {
    return set(new under18.SimpleRecord());
  }

  @Benchmark
  public SpecificRecord compatVanilla19() {
    return set(new under19.SimpleRecord());
  }

  @Benchmark
  public SpecificRecord compatVanilla110() {
    return set(new under110.SimpleRecord());
  }

  @Benchmark
  public SpecificRecord compatVanilla111() {
    return set(new under111.SimpleRecord());
  }

  @Benchmark
  public SpecificRecord compatBuilder17() {
    return under17target17.SimpleRecord.newBuilder().setStringField(MESSAGE).build();
  }

  @Benchmark
  public SpecificRecord compatBuilder18() {
    return under18wbuilders.SimpleRecord.newBuilder().setStringField(MESSAGE).build();
  }

  @Benchmark
  public SpecificRecord compatBuilder19() {
    return under19wbuilders.SimpleRecord.newBuilder().setStringField(MESSAGE).build();
  }

  @Benchmark
  public SpecificRecord compatBuilder110() {
    return under110wbuilders.SimpleRecord.newBuilder().setStringField(MESSAGE).build();
  }
}
