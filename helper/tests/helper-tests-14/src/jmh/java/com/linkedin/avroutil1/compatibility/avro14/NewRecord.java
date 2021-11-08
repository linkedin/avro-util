/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro14;

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

  // A baseline to compare the rest of the measurements against.
  // Useful as a ceiling of the possible performance and to get
  // an idea of the inherent overhead/variance involved.
  @Benchmark
  public SpecificRecord baseline() {
    return null;
  }

  @Benchmark
  public SpecificRecord vanillaDirect14() {
    return set(new by14.SimpleRecord());
  }

  @Benchmark
  public SpecificRecord vanillaDirect15() {
    return set(new by15.SimpleRecord());
  }

  @Benchmark
  public SpecificRecord processedDirect14() {
    return set(new under14.SimpleRecord());
  }

  @Benchmark
  public SpecificRecord processedDirect15() {
    return set(new under15.SimpleRecord());
  }

  @Benchmark
  public SpecificRecord processedDirect16() {
    return set(new under16.SimpleRecord());
  }

  @Benchmark
  public SpecificRecord processedDirect17() {
    return set(new under17.SimpleRecord());
  }

  @Benchmark
  public SpecificRecord processedDirect18() {
    return set(new under18.SimpleRecord());
  }

  @Benchmark
  public SpecificRecord processedDirect19() {
    return set(new under19.SimpleRecord());
  }

  @Benchmark
  public SpecificRecord processedDirect110() {
    return set(new under110.SimpleRecord());
  }

  @Benchmark
  public SpecificRecord processedDirect111() {
    return set(new under111.SimpleRecord());
  }
}
