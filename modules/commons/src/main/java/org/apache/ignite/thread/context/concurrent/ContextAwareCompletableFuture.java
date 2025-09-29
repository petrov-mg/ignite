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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.thread.context.function.ContextAwareBiConsumer;
import org.apache.ignite.thread.context.function.ContextAwareBiFunction;
import org.apache.ignite.thread.context.function.ContextAwareConsumer;
import org.apache.ignite.thread.context.function.ContextAwareFunction;
import org.apache.ignite.thread.context.function.ContextAwareRunnable;

/** */
public class ContextAwareCompletableFuture<T> extends CompletableFuture<T> {
    /** {@inheritDoc} */
    @Override public <U> CompletableFuture<U> thenApply(Function<? super T, ? extends U> fn) {
        return super.thenApply(ContextAwareFunction.wrap(fn));
    }

    /** {@inheritDoc} */
    @Override public <U> CompletableFuture<U> thenApplyAsync(Function<? super T, ? extends U> fn) {
        return super.thenApplyAsync(ContextAwareFunction.wrap(fn));
    }

    /** {@inheritDoc} */
    @Override public <U> CompletableFuture<U> thenApplyAsync(Function<? super T, ? extends U> fn, Executor executor) {
        return super.thenApplyAsync(ContextAwareFunction.wrap(fn), executor);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> thenAccept(Consumer<? super T> action) {
        return super.thenAccept(ContextAwareConsumer.wrap(action));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> thenAcceptAsync(Consumer<? super T> action) {
        return super.thenAcceptAsync(ContextAwareConsumer.wrap(action));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> thenAcceptAsync(Consumer<? super T> action, Executor executor) {
        return super.thenAcceptAsync(ContextAwareConsumer.wrap(action), executor);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> thenRun(Runnable action) {
        return super.thenRun(ContextAwareRunnable.wrap(action));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> thenRunAsync(Runnable action) {
        return super.thenRunAsync(ContextAwareRunnable.wrap(action));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> thenRunAsync(Runnable action, Executor executor) {
        return super.thenRunAsync(ContextAwareRunnable.wrap(action), executor);
    }

    /** {@inheritDoc} */
    @Override public <U, V> CompletableFuture<V> thenCombine(
        CompletionStage<? extends U> other,
        BiFunction<? super T, ? super U, ? extends V> fn
    ) {
        return super.thenCombine(other, ContextAwareBiFunction.wrap(fn));
    }

    /** {@inheritDoc} */
    @Override public <U, V> CompletableFuture<V> thenCombineAsync(
        CompletionStage<? extends U> other,
        BiFunction<? super T, ? super U, ? extends V> fn
    ) {
        return super.thenCombineAsync(other, ContextAwareBiFunction.wrap(fn));
    }

    /** {@inheritDoc} */
    @Override public <U, V> CompletableFuture<V> thenCombineAsync(
        CompletionStage<? extends U> other,
        BiFunction<? super T, ? super U, ? extends V> fn,
        Executor executor
    ) {
        return super.thenCombineAsync(other, ContextAwareBiFunction.wrap(fn), executor);
    }

    /** {@inheritDoc} */
    @Override public <U> CompletableFuture<Void> thenAcceptBoth(
        CompletionStage<? extends U> other,
        BiConsumer<? super T, ? super U> action
    ) {
        return super.thenAcceptBoth(other, ContextAwareBiConsumer.wrap(action));
    }

    /** {@inheritDoc} */
    @Override public <U> CompletableFuture<Void> thenAcceptBothAsync(
        CompletionStage<? extends U> other,
        BiConsumer<? super T, ? super U> action
    ) {
        return super.thenAcceptBoth(other, ContextAwareBiConsumer.wrap(action));
    }

    /** {@inheritDoc} */
    @Override public <U> CompletableFuture<Void> thenAcceptBothAsync(
        CompletionStage<? extends U> other,
        BiConsumer<? super T, ? super U> action,
        Executor executor
    ) {
        return super.thenAcceptBothAsync(other, ContextAwareBiConsumer.wrap(action), executor);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
        return super.runAfterBoth(other, ContextAwareRunnable.wrap(action));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
        return super.runAfterBothAsync(other, ContextAwareRunnable.wrap(action));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return super.runAfterBothAsync(other, ContextAwareRunnable.wrap(action), executor);
    }

    /** {@inheritDoc} */
    @Override public <U> CompletableFuture<U> applyToEither(CompletionStage<? extends T> other, Function<? super T, U> fn) {
        return super.applyToEither(other, ContextAwareFunction.wrap(fn));
    }

    /** {@inheritDoc} */
    @Override public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn) {
        return super.applyToEitherAsync(other, ContextAwareFunction.wrap(fn));
    }

    /** {@inheritDoc} */
    @Override public <U> CompletableFuture<U> applyToEitherAsync(
        CompletionStage<? extends T> other,
        Function<? super T, U> fn,
        Executor executor
    ) {
        return super.applyToEitherAsync(other, ContextAwareFunction.wrap(fn), executor);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> acceptEither(CompletionStage<? extends T> other, Consumer<? super T> action) {
        return super.acceptEither(other, ContextAwareConsumer.wrap(action));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action) {
        return super.acceptEitherAsync(other, ContextAwareConsumer.wrap(action));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> acceptEitherAsync(
        CompletionStage<? extends T> other,
        Consumer<? super T> action,
        Executor executor
    ) {
        return super.acceptEitherAsync(other, ContextAwareConsumer.wrap(action), executor);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
        return super.runAfterEither(other, ContextAwareRunnable.wrap(action));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action) {
        return super.runAfterEitherAsync(other, ContextAwareRunnable.wrap(action));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return super.runAfterEitherAsync(other, ContextAwareRunnable.wrap(action), executor);
    }

    /** {@inheritDoc} */
    @Override public <U> CompletableFuture<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> fn) {
        return super.thenCompose(ContextAwareFunction.wrap(fn));
    }

    /** {@inheritDoc} */
    @Override public <U> CompletableFuture<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn) {
        return super.thenComposeAsync(ContextAwareFunction.wrap(fn));
    }

    /** {@inheritDoc} */
    @Override public <U> CompletableFuture<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn, Executor executor) {
        return super.thenComposeAsync(ContextAwareFunction.wrap(fn), executor);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<T> whenComplete(BiConsumer<? super T, ? super Throwable> action) {
        return super.whenComplete(ContextAwareBiConsumer.wrap(action));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action) {
        return super.whenCompleteAsync(ContextAwareBiConsumer.wrap(action));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action, Executor executor) {
        return super.whenCompleteAsync(ContextAwareBiConsumer.wrap(action), executor);
    }

    /** {@inheritDoc} */
    @Override public <U> CompletableFuture<U> handle(BiFunction<? super T, Throwable, ? extends U> fn) {
        return super.handle(ContextAwareBiFunction.wrap(fn));
    }

    /** {@inheritDoc} */
    @Override public <U> CompletableFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn) {
        return super.handleAsync(ContextAwareBiFunction.wrap(fn));
    }

    /** {@inheritDoc} */
    @Override public <U> CompletableFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn, Executor executor) {
        return super.handleAsync(ContextAwareBiFunction.wrap(fn), executor);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<T> exceptionally(Function<Throwable, ? extends T> fn) {
        return super.exceptionally(ContextAwareFunction.wrap(fn));
    }

    /** {@inheritDoc} */
    @Override public <U> CompletableFuture<U> newIncompleteFuture() {
        return new ContextAwareCompletableFuture<>();
    }
}

