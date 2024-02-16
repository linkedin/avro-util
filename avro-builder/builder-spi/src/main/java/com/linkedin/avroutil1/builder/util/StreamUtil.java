/*
 * Copyright 2024 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
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
    return toParallelStream(mapper, parallelism, 1);
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
    return new ParallelStreamCollector<>(mapper, parallelism, batchSize);
  }

  private final static class LimitingExecutor implements Executor {
    private final Semaphore _limiter;

    private LimitingExecutor(int maxParallelism) {
      _limiter = new Semaphore(maxParallelism);
    }

    @Override
    public void execute(Runnable command) {
      try {
        _limiter.acquire();
        WORK_EXECUTOR.execute(() -> {
          try {
            command.run();
          } finally {
            _limiter.release();
          }
        });
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static final class ParallelStreamCollector<T, R> implements Collector<T, LinkedList<T>, Stream<R>> {
    private final int _batchSize;
    private final Function<T, R> _mapper;
    private final Executor _executor;
    private final List<CompletableFuture<List<R>>> _futures = new ArrayList<>();

    private ParallelStreamCollector(Function<T, R> mapper, int parallelism, int batchSize) {
      if (parallelism <= 0 || batchSize <= 0) {
        throw new IllegalArgumentException("Parallelism and batch size must be > 0");
      }
      _mapper = mapper;
      _batchSize = batchSize;
      _executor = new LimitingExecutor(parallelism);
    }

    @Override
    public Supplier<LinkedList<T>> supplier() {
      return LinkedList::new;
    }

    public BiConsumer<LinkedList<T>, T> accumulator() {
      return this::accumulate;
    }

    private void accumulate(LinkedList<T> list, T element) {
      if (list.size() >= _batchSize) {
        List<T> listCopy = new ArrayList<>(list);
        _futures.add(CompletableFuture.supplyAsync(() -> listCopy.stream().map(_mapper).collect(Collectors.toList()),
            _executor));
        list.clear();
      }
      list.add(element);
    }

    @Override
    public BinaryOperator<LinkedList<T>> combiner() {
      return (left, right) -> {
        left.addAll(right);
        return left;
      };
    }

    @Override
    public Function<LinkedList<T>, Stream<R>> finisher() {
      return list -> {
        if (!list.isEmpty()) {
          _futures.add(
              CompletableFuture.supplyAsync(() -> list.stream().map(_mapper).collect(Collectors.toList()), _executor));
        }

        return _futures.stream().flatMap(future -> future.join().stream());
      };
    }

    @Override
    public Set<Characteristics> characteristics() {
      return Collections.singleton(Characteristics.UNORDERED);
    }
  }
}
