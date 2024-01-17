package com.linkedin.avroutil1.builder.util;

import com.pivovarit.collectors.ParallelCollectors;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collector;
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
      new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

  private StreamUtil() {
    // Disallow external instantiation.
  }

  /**
   * A convenience {@link Collector} used for executing parallel computations on a custom {@link Executor}
   * and returning a {@link Stream} instance returning results as they arrive.
   * <p>
   * For the parallelism of 1, the stream is executed by the calling thread.
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
}
