package org.infinispan.commons.util.concurrent;


import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

import org.infinispan.commons.reactive.RxJavaInterop;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Scheduler;

/**
 * Utility methods for handling {@link CompletionStage} instances.
 * @author wburns
 * @since 10.0
 */
public class CompletionStages {

   public static final Runnable NO_OP_RUNNABLE = () -> {};

   private CompletionStages() { }


   /**
    * Returns a CompletionStage that also can be composed of many other CompletionStages. A stage can compose another
    * stage in it by invoking the {@link AggregateCompletionStage#dependsOn(CompletionStage)} method passing in the
    * CompletionStage. After all stages this composition stage depend upon have been added, the
    * {@link AggregateCompletionStage#freeze()} should be invoked so that the AggregateCompletionStage can finally
    * complete when all of the stages it depends upon complete.
    * <p>
    * If any stage this depends upon fails the returned stage will contain the Throwable from one of the stages.
    * @return composed completion stage
    */
   public static AggregateCompletionStage<Void> aggregateCompletionStage() {
      return new VoidAggregateCompletionStage();
   }

   /**
    * Same as {@link #aggregateCompletionStage()} except that when this stage completes normally it will return
    * the value provided.
    * @param valueToReturn value to return to future stage compositions
    * @param <R> the type of the value
    * @return composed completion stage that returns the value upon normal completion
    */
   public static <R> AggregateCompletionStage<R> aggregateCompletionStage(R valueToReturn) {
      return new ValueAggregateCompletionStage<>(valueToReturn);
   }

   public static AggregateCompletionStage<Boolean> orBooleanAggregateCompletionStage() {
      return new OrBooleanAggregateCompletionStage();
   }

   /**
    * Returns if the provided {@link CompletionStage} has already completed normally, that is not due to an exception.
    * @param stage stage to check
    * @return if the stage is completed normally
    */
   public static boolean isCompletedSuccessfully(CompletionStage<?> stage) {
      CompletableFuture<?> future = stage.toCompletableFuture();
      return future.isDone() && !future.isCompletedExceptionally();
   }

   /**
    * Returns the result value when complete, or throws an (unchecked) exception if completed exceptionally.
    * To better conform with the use of common functional forms, if a computation involved in the completion of this
    * CompletionStage threw an exception, this method throws an (unchecked) CompletionException with the underlying
    * exception as its cause.
    * @param stage stage to wait on
    * @param <R> the type in the stage
    * @return the result value
    * @throws CompletionException if this stage completed exceptionally or a completion computation threw an exception
    */
   public static <R> R join(CompletionStage<R> stage) {
      try {
         return CompletableFutures.await(stage.toCompletableFuture());
      } catch (ExecutionException e) {
         throw new CompletionException(e.getCause());
      } catch (InterruptedException e) {
         throw new CompletionException(e);
      }
   }

   /**
    * Returns a CompletableStage that completes when both of the provides CompletionStages complete. This method
    * may choose to return either of the argument if the other is complete or a new instance completely.
    * @param first the first CompletionStage
    * @param second the second CompletionStage
    * @return a CompletionStage that is complete when both of the given CompletionStages complete
    */
   public static CompletionStage<Void> allOf(CompletionStage<Void> first, CompletionStage<Void> second) {
      if (!isCompletedSuccessfully(first)) {
         if (isCompletedSuccessfully(second)) {
            return first;
         } else {
            return CompletionStages.aggregateCompletionStage().dependsOn(first).dependsOn(second).freeze();
         }
      }
      return second;
   }

   /**
    * Returns a CompletionStage that completes when all of the provided stages complete, either normally or via
    * exception. If one or more states complete exceptionally the returned CompletionStage will complete with the
    * exception of one of these. If no CompletionStages are provided, returns a CompletionStage completed with the value
    * null.
    * @param stages the CompletionStages
    * @return a CompletionStage that is completed when all of the given CompletionStages complete
    */
   public static CompletionStage<Void> allOf(CompletionStage<?>... stages) {
      AggregateCompletionStage<Void> aggregateCompletionStage = null;
      for (CompletionStage<?> stage : stages) {
         if (!isCompletedSuccessfully(stage)) {
            if (aggregateCompletionStage == null) {
               aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
            }
            aggregateCompletionStage.dependsOn(stage);
         }
      }

      return aggregateCompletionStage != null ? aggregateCompletionStage.freeze() : CompletableFutures.completedNull();
   }

   /**
    * Extend {@link CompletionStage#thenCompose(Function)} to also handle exceptions.
    */
   public static <T, U> CompletionStage<U> handleAndCompose(CompletionStage<T> stage,
                                                            BiFunction<T, Throwable, CompletionStage<U>> handleFunction) {
      if (isCompletedSuccessfully(stage)) {
         T value = join(stage);
         try {
            return handleFunction.apply(value, null);
         } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
         }
      }
      return stage.handle(handleFunction).thenCompose(Function.identity());
   }

   public static <T, U> CompletionStage<U> handleAndComposeAsync(CompletionStage<T> stage,
         BiFunction<T, Throwable, CompletionStage<U>> handleFunction, Executor executor) {
      return stage.handleAsync(handleFunction, executor).thenCompose(Function.identity());
   }

   public static CompletionStage<Void> schedule(Runnable command, ScheduledExecutorService executor,
         long delay, TimeUnit timeUnit) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      executor.schedule(() -> {
         try {
            command.run();
            future.complete(null);
         } catch (Throwable t) {
            future.completeExceptionally(t);
         }
      }, delay, timeUnit);
      return future;
   }

   public static <T> CompletionStage<T> schedule(Callable<T> command, ScheduledExecutorService executor,
                                                   long delay, TimeUnit timeUnit) {
      CompletableFuture<T> future = new CompletableFuture<>();
      executor.schedule(() -> {
         try {
            T value = command.call();
            future.complete(value);
         } catch (Throwable t) {
            future.completeExceptionally(t);
         }
      }, delay, timeUnit);
      return future;
   }

   public static <T> CompletionStage<T> scheduleNonBlocking(Callable<? extends CompletionStage<T>> command,
                                                            ScheduledExecutorService executor,
                                                            long delay, TimeUnit timeUnit) {
      return schedule(command, executor, delay, timeUnit).thenCompose(Function.identity());
   }

   public static CompletionStage<Void> ignoreValue(CompletionStage<?> stage) {
      return stage.thenRun(NO_OP_RUNNABLE);
   }

   public static <T> T await(CompletionStage<T> stage) throws ExecutionException, InterruptedException {
      return CompletableFutures.await(stage.toCompletableFuture());
   }

   private static class VoidAggregateCompletionStage extends AbstractAggregateCompletionStage<Void> {
      @Override
      Void getValue() {
         return null;
      }
   }

   private static class ValueAggregateCompletionStage<R> extends AbstractAggregateCompletionStage<R> {
      private final R value;

      private ValueAggregateCompletionStage(R value) {
         this.value = value;
      }

      @Override
      R getValue() {
         return value;
      }
   }

   private static class OrBooleanAggregateCompletionStage extends AbstractAggregateCompletionStage<Boolean> {

      private volatile boolean value = false;

      @Override
      Boolean getValue() {
         return value;
      }

      @Override
      public void accept(Object o, Throwable t) {
         if (t != null) {
            super.accept(null, t);
            return;
         }
         if (o instanceof Boolean && (Boolean) o) {
            this.value = true;
         }
         super.accept(o, null);
      }
   }

   /**
    * Abstract {@link AggregateCompletionStage} that will keep a count of non completed stages it depends upon while
    * only registering to be notified when each completes, decrementing the counter. The returned CompletionStage
    * via {@link #freeze()} will be completed when the counter is zero, providing the value returned from
    * {@link #getValue()} as the result.
    * This class implements BiConsumer and extends CompletableFuture to avoid additional object/lambda allocation per instance
    * @param <R>
    */
   private abstract static class AbstractAggregateCompletionStage<R> extends CompletableFuture<R>
         implements AggregateCompletionStage<R>, BiConsumer<Object, Throwable> {
      private static final AtomicIntegerFieldUpdater<AbstractAggregateCompletionStage> remainingUpdater =
            AtomicIntegerFieldUpdater.newUpdater(AbstractAggregateCompletionStage.class, "remaining");

      @SuppressWarnings({"unused"})
      private volatile int remaining;
      private volatile boolean frozen = false;
      private volatile Throwable throwable;

      @Override
      public void accept(Object o, Throwable t) {
         if (t != null) {
            throwable = t;
         }
         if (remainingUpdater.decrementAndGet(this) == 0 && frozen) {
            complete();
         }
      }

      @Override
      public final AggregateCompletionStage<R> dependsOn(CompletionStage<?> stage) {
         Objects.requireNonNull(stage);
         if (frozen) {
            throw new IllegalStateException();
         }
         // We only depend upon it if the stage wasn't complete
         if (!isCompletedSuccessfully(stage)) {
            remainingUpdater.incrementAndGet(this);
            stage.whenComplete(this);
         }
         return this;
      }

      @Override
      public final CompletionStage<R> freeze() {
         frozen = true;
         if (remainingUpdater.get(this) == 0) {
            complete();
         }
         return this;
      }

      private void complete() {
         Throwable t = throwable;
         if (t != null) {
            completeExceptionally(t);
         } else {
            complete(getValue());
         }
      }

      abstract R getValue();
   }

   public static <I> CompletionStage<Void> performConcurrently(Iterable<I> iterable, int parallelism, Scheduler scheduler,
                                                               Function<? super I, CompletionStage<?>> function) {
      return performConcurrently(Flowable.fromIterable(iterable), parallelism, scheduler, function);
   }

   public static <I> CompletionStage<Void> performConcurrently(Stream<I> stream, int parallelism, Scheduler scheduler,
                                                               Function<? super I, CompletionStage<?>> function) {
      return performConcurrently(Flowable.fromStream(stream), parallelism, scheduler, function);
   }

   private static <I> CompletionStage<Void> performConcurrently(Flowable<I> flowable, int parallelism, Scheduler scheduler,
                                                                Function<? super I, CompletionStage<?>> function) {
      return flowable
            .parallel(parallelism)
            .runOn(scheduler)
            .concatMap(i -> RxJavaInterop.voidCompletionStageToFlowable(function.apply(i)))
            .sequential()
            .ignoreElements().toCompletionStage(null);
   }

   public static <I, T, A, R> CompletionStage<R> performConcurrently(Iterable<I> iterable, int parallelism, Scheduler scheduler,
                                                                     Function<? super I, CompletionStage<T>> function,
                                                                     Collector<T, A, R> collector) {
      return performConcurrently(Flowable.fromIterable(iterable), parallelism, scheduler, function, collector);
   }

   public static <I, T, A, R> CompletionStage<R> performConcurrently(Stream<I> stream, int parallelism, Scheduler scheduler,
                                                                     Function<? super I, CompletionStage<T>> function,
                                                                     Collector<T, A, R> collector) {
      return performConcurrently(Flowable.fromStream(stream), parallelism, scheduler, function, collector);
   }

   private static <I, T, A, R> CompletionStage<R> performConcurrently(Flowable<I> flowable, int parallelism, Scheduler scheduler,
                                                                      Function<? super I, CompletionStage<T>> function, Collector<T, A, R> collector) {
      return flowable
            .parallel(parallelism)
            .runOn(scheduler)
            .concatMap(i -> Flowable.fromCompletionStage(function.apply(i)))
            .collect(collector)
            .singleOrErrorStage();
   }

   public static <I> CompletionStage<Void> performSequentially(Iterator<I> iterator, Function<? super I, CompletionStage<Void>> function) {
      return performSequentially(iterator, function, null, (ignore1, ignore2) -> {});
   }

   public static <I, T, A, R> CompletionStage<R> performSequentially(Iterator<I> iterator, Function<? super I, CompletionStage<T>> function, Collector<T, A, R> collector) {
      A supplier = collector.supplier().get();
      CompletionStage<A> stage =  performSequentially(iterator, function, supplier, collector.accumulator());
      return stage.thenApply(collector.finisher());
   }

   private static <I, T, A> CompletionStage<A> performSequentially(Iterator<I> iterator, Function<? super I,
         CompletionStage<T>> function, A collected, BiConsumer<A, T> accumulator) {
      CompletionStage<Void> stage = CompletableFutures.completedNull();
      // Replace recursion with iteration if the state was applied synchronously
      while (iterator.hasNext() && CompletionStages.isCompletedSuccessfully(stage)) {
         I value = iterator.next();
         stage = function.apply(value)
               .thenAccept(t -> accumulator.accept(collected, t));
      }
      if (!iterator.hasNext())
         return stage.thenApply(t -> collected);

      return stage.thenCompose(v -> performSequentially(iterator, function, collected, accumulator));
   }
}
