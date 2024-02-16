/*
 * Copyright 2024 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.util;

import com.pivovarit.collectors.ParallelCollectors;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;


/**
 * Utilities for dealing with java streams.
 */
public final class StreamUtil {

  /**
   * An (effectively) unbounded {@link ExecutorService} used for parallel processing. This is kept unbounded to avoid
   * deadlocks caused when using {@link #toParallelStream(Function, int)} recursively. Callers are supposed to set
   * sane values for parallelism to avoid spawning a crazy number of concurrent threads.
   */
  private static final ExecutorService WORK_EXECUTOR =
      new ThreadPoolExecutor(0, Integer.MAX_VALUE, 100, TimeUnit.MILLISECONDS, new SynchronousQueue<>());

  private StreamUtil() {
    // Disallow external instantiation.
  }

  /**
   * A convenience {@link Collector} used for executing parallel computations on a custom {@link Executor}
   * and returning a {@link Stream} instance returning results as they arrive.
   *
   * <p>For the parallelism of 1, the stream is executed by the calling thread.</p>
   *
   * @param mapper      a transformation to be performed in parallel
   * @param parallelism the max parallelism level
   * @param <T>         the type of the collected elements
   * @param <R>         the result returned by {@code mapper}
   *
   * @return a {@code Collector} which collects all processed elements into a {@code Stream} in parallel.
   */
  public static <T, R> Collector<T, ?, Stream<R>> toParallelStream(Function<T, R> mapper, int parallelism) {
    return ParallelCollectors.parallelToStream(mapper, WORK_EXECUTOR, parallelism);
  }

  /**
   * A convenience {@link Collector} used for executing parallel computations on a custom {@link Executor}
   * and returning a {@link Stream} instance returning results as they arrive.
   *
   * <p>For the parallelism of 1 or if the size of the elements is &lt;= batchSize, the stream is executed by the
   * calling thread.</p>
   *
   * @param mapper      a transformation to be performed in parallel
   * @param parallelism the max parallelism level
   * @param batchSize   the size into which inputs should be batched before running the mapper.
   * @param <T>         the type of the collected elements
   * @param <R>         the result returned by {@code mapper}
   *
   * @return a {@code Collector} which collects all processed elements into a {@code Stream} in parallel.
   */
  public static <T, R> Collector<T, ?, Stream<R>> toParallelStream(Function<T, R> mapper, int parallelism,
      int batchSize) {
    // When batch size is 1, fallback to toParallelStream
    if (batchSize == 1) {
      return toParallelStream(mapper, parallelism);
    }

    return Collectors.collectingAndThen(Collectors.toList(), list -> {
      if (list.isEmpty()) {
        return Stream.empty();
      }

      if (parallelism == 1 || list.size() <= batchSize) {
        return list.stream().map(mapper);
      }

      final int batchCount = (list.size() - 1) / batchSize;
      final Function<List<T>, List<R>> batchingMapper =
          batch -> batch.stream().map(mapper).collect(Collectors.toList());
      List<List<T>> sublists = IntStream.rangeClosed(0, batchCount).mapToObj(batch -> {
        int startIndex = batch * batchSize;
        int endIndex = (batch == batchCount) ? list.size() : (batch + 1) * batchSize;
        return list.subList(startIndex, endIndex);
      }).collect(Collectors.toList());

      return sublists.stream().collect(toParallelStream(batchingMapper, parallelism)).flatMap(List::stream);
    });
  }
}
