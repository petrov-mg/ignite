/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.thread.context.concurrent;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import org.apache.ignite.thread.context.function.ContextAwareCallable;
import org.apache.ignite.thread.context.function.ContextAwareRunnable;
import org.jetbrains.annotations.NotNull;

/** */
public class ContextAwareForkJoinPool extends AbstractExecutorService {
    /** */
    private static final ContextAwareForkJoinPool COMMON = new ContextAwareForkJoinPool(ForkJoinPool.commonPool());

    /** */
    private final ForkJoinPool delegate;

    /** */
    public ContextAwareForkJoinPool() {
        this(new ForkJoinPool());
    }

    /** */
    public ContextAwareForkJoinPool(int parallelism) {
        this(new ForkJoinPool(parallelism));
    }

    /** */
    public ContextAwareForkJoinPool(
        int parallelism,
        ForkJoinPool.ForkJoinWorkerThreadFactory factory,
        Thread.UncaughtExceptionHandler handler,
        boolean asyncMode
    ) {
        this(new ForkJoinPool(parallelism, factory, handler, asyncMode));
    }

    /** */
    public ContextAwareForkJoinPool(
        int parallelism,
        ForkJoinPool.ForkJoinWorkerThreadFactory factory,
        Thread.UncaughtExceptionHandler handler,
        boolean asyncMode,
        int corePoolSize,
        int maximumPoolSize,
        int minimumRunnable,
        Predicate<? super ForkJoinPool> saturate,
        long keepAliveTime,
        TimeUnit unit
    ) {
        this(new ForkJoinPool(parallelism, factory, handler, asyncMode, corePoolSize, maximumPoolSize, minimumRunnable, saturate, keepAliveTime, unit));
    }

    /** */
    private ContextAwareForkJoinPool(ForkJoinPool delegate) {
        this.delegate = delegate;
    }

    /** */
    public <T> T invoke(ForkJoinTask<T> task) {
        throw new UnsupportedOperationException();
    }

    /** */
    public void execute(ForkJoinTask<?> task) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void execute(Runnable task) {
        delegate.execute(ContextAwareRunnable.wrap(task));
    }

    /** */
    public <T> ForkJoinTask<T> submit(ForkJoinTask<T> task) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public <T> ForkJoinTask<T> submit(Callable<T> task) {
        return delegate.submit(ContextAwareCallable.wrap(task));
    }

    /** {@inheritDoc} */
    @Override public <T> ForkJoinTask<T> submit(Runnable task, T result) {
        return delegate.submit(ContextAwareRunnable.wrap(task), result);
    }

    /** {@inheritDoc} */
    @Override public ForkJoinTask<?> submit(Runnable task) {
        return delegate.submit(ContextAwareRunnable.wrap(task));
    }

    /** {@inheritDoc} */
    @Override public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) {
        return delegate.invokeAll(ContextAwareCallable.wrap(tasks));
    }

    /** */
    public ForkJoinPool.ForkJoinWorkerThreadFactory getFactory() {
        return delegate.getFactory();
    }

    /** */
    public Thread.UncaughtExceptionHandler getUncaughtExceptionHandler() {
        return delegate.getUncaughtExceptionHandler();
    }

    /** */
    public int getParallelism() {
        return delegate.getParallelism();
    }

    /** */
    public int getPoolSize() {
        return delegate.getPoolSize();
    }

    /** */
    public boolean getAsyncMode() {
        return delegate.getAsyncMode();
    }

    /** */
    public int getRunningThreadCount() {
        return delegate.getRunningThreadCount();
    }

    /** */
    public int getActiveThreadCount() {
        return delegate.getActiveThreadCount();
    }

    /** */
    public boolean isQuiescent() {
        return delegate.isQuiescent();
    }

    /** */
    public long getStealCount() {
        return delegate.getStealCount();
    }

    /** */
    public long getQueuedTaskCount() {
        return delegate.getQueuedTaskCount();
    }

    /** */
    public int getQueuedSubmissionCount() {
        return delegate.getQueuedSubmissionCount();
    }

    /** */
    public boolean hasQueuedSubmissions() {
        return delegate.hasQueuedSubmissions();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return delegate.toString();
    }

    /** {@inheritDoc} */
    @Override public void shutdown() {
        delegate.shutdown();
    }

    /** {@inheritDoc} */
    @Override public List<Runnable> shutdownNow() {
        return delegate.shutdownNow();
    }

    /** {@inheritDoc} */
    @Override public boolean isTerminated() {
        return delegate.isTerminated();
    }

    /** */
    public boolean isTerminating() {
        return delegate.isTerminating();
    }

    /** {@inheritDoc} */
    @Override public boolean isShutdown() {
        return delegate.isShutdown();
    }

    /** {@inheritDoc} */
    @Override public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.awaitTermination(timeout, unit);
    }

    /** */
    public boolean awaitQuiescence(long timeout, TimeUnit unit) {
        return delegate.awaitQuiescence(timeout, unit);
    }

    /** {@inheritDoc} */
    @Override public <T> T invokeAny(
        @NotNull Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return delegate.invokeAny(ContextAwareCallable.wrap(tasks));
    }

    /** {@inheritDoc} */
    @Override public <T> T invokeAny(
        @NotNull Collection<? extends Callable<T>> tasks,
        long timeout,
        @NotNull TimeUnit unit
    ) throws InterruptedException, ExecutionException, TimeoutException {
        return delegate.invokeAny(ContextAwareCallable.wrap(tasks), timeout, unit);
    }

    /** {@inheritDoc} */
    @NotNull @Override public <T> List<Future<T>> invokeAll(
        @NotNull Collection<? extends Callable<T>> tasks,
        long timeout,
        @NotNull TimeUnit unit
    ) throws InterruptedException {
        return delegate.invokeAll(ContextAwareCallable.wrap(tasks), timeout, unit);
    }

    /** */
    public static ContextAwareForkJoinPool commonPool() {
        return COMMON;
    }
}
