package org.infinispan.util.concurrent;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import org.reactivestreams.Publisher;

/**
 * Manager utility for blocking operations that runs tasks on the blocking executor and returns a
 * {@code CompletionStage} or {@code Publisher} that continues on the non-blocking executor, similar
 * to {@code stage.handleAsync(callback, blockingExecutor).whenCompleteAsync(NOOP, nonBlockingExecutor)}.
 * <p>
 * If the current thread is blocking, it blocks until the task can run, then runs the task in the current thread and returns a
 * completed {@code CompletionStage} so it <em>does not</em> continue the execution on the non-blocking executor.
 * <p>
 * Many of the methods on {@code BlockingManager} let you pass an identifier (ID) when performing the operation. This ID is
 * printed with TRACE logs. For this reason, you should provide IDs that are unique, making it easier to track the stream
 * of operations across threads if TRACE logs are used.
 */
public interface BlockingManager {
   /**
    * Replacement for {@code CompletionStage.runAsync()} that invokes the {@code Runnable} in a blocking thread
    * if the current thread is non-blocking or in the current thread if the current thread is blocking.
    * The returned stage, if not complete, resumes any chained stage on the non-blocking executor.
    * <p>
    * Note that if the current thread is blocking, the task is invoked in the current thread, meaning the stage is
    * always completed when returned, so any chained stage is also invoked on the current thread.
    * @param runnable blocking operation that runs some code.
    * @param traceId an identifier that can be used to tell in a trace when an operation moves between threads.
    * @return a stage that is completed after the runnable is done or throws an exception.
    */
   CompletionStage<Void> runBlocking(Runnable runnable, Object traceId);

   /**
    * Subscribes to the provided publisher on the invoking thread. Published values are observed on a blocking thread
    * one a time passed to the provided consumer. The returned stage if not complete will resume any chained stage
    * on the non blocking executor.
    * <p>
    * If no values are published the returned stage will be completed upon return of this method and require no
    * thread context switches
    * <p>
    * Note that if the current thread is blocking everything including subscription, publication and consumption of
    * values will be done on the current thread.
    * @param publisher publisher of values to consume
    * @param consumer consumer to handle the values
    * @param traceId an identifier that can be used to tell in a trace when an operation moves between threads
    * @param <E> the type of entries
    * @return a stage that is completed after all values are consumed
    */
   <E> CompletionStage<Void> subscribeBlockingConsumer(Publisher<E> publisher, Consumer<E> consumer, Object traceId);

   /**
    * Subscribes to the provided publisher on the invoking thread. Published values are observed on a blocking thread
    * one a time passed to the provided collector. The returned stage if not complete will resume any chained stage
    * on the non blocking executor.
    * <p>
    * If no values are published the returned stage will be completed upon return of this method and require no
    * thread context switches
    * <p>
    * Note that if the current thread is blocking everything including subscription, publication and collection of
    * values will be done on the current thread.
    * @param publisher publisher of values to collect
    * @param collector collector of the values
    * @param traceId an identifier that can be used to tell in a trace when an operation moves between threads
    * @param <T> the type of entries
    * @param <A> accumulator type of the entries
    * @param <R> final value type
    * @return a stage that when complete contains the collected values as a single value
    */
   <T, A, R> CompletionStage<R> subscribeBlockingCollector(Publisher<T> publisher, Collector<? super T, A, R> collector,
         Object traceId);

   /**
    * Replacement for {@code CompletionStage.supplyAsync()} that invokes the {@code Supplier} in a blocking thread
    * (if the current thread is non-blocking) or in the current thread (if the current thread is blocking).
    * The returned stage, if not complete, resumes any chained stage on the non-blocking executor.
    * <p>
    * Note that if the current thread is blocking, the task is invoked in the current thread meaning the stage is
    * always completed when returned, so any chained stage is also invoked on the current thread.
    * @param <V> the supplied type.
    * @param supplier blocking operation that returns a value.
    * @param traceId an identifier that can be used to tell in a trace when an operation moves between threads.
    * @return a stage that, when complete, contains the value returned from the supplier or a throwable.
    */
   <V> CompletionStage<V> supplyBlocking(Supplier<V> supplier, Object traceId);

   /**
    * Replacement for {@code CompletionStage.handleAsync()} that invokes the {@code BiFunction} in a blocking thread
    * (if the current thread is non-blocking) or in the current thread (if the current thread is blocking).
    * The returned stage, if not complete, resumes any chained stage on the non-blocking executor.
    * <p>
    * Note that if the current thread is blocking, the task is invoked in the current thread meaning the stage is
    * always completed when returned, so any chained stage is also invoked on the current thread.
    * @param stage stage, that may or may not be complete, to handle.
    * @param function the blocking function.
    * @param traceId an identifier that can be used to tell in a trace when an operation moves between threads.
    * @param <I> input value type to the function.
    * @param <O> output value type after being transformed via function.
    * @return a stage that, when complete, contains the value returned from the function or a throwable.
    */
   <I, O> CompletionStage<O> handleBlocking(CompletionStage<? extends I> stage,
         BiFunction<? super I, Throwable, ? extends O> function, Object traceId);

   /**
    * Replacement for {@link CompletionStage#thenRunAsync(Runnable)} that invokes the {@code Runnable} in a blocking thread
    * (if the current thread is non-blocking) or in the current thread (if the current thread is blocking).
    * The returned stage, if not complete, resumes any chained stage on the non-blocking executor.
    * <p>
    * Note that if the current thread is blocking, the task is invoked in the current thread meaning the stage is
    * always completed when returned, so any chained stage is also invoked on the current thread.
    * @param stage stage, that may or may not be complete, to apply.
    * @param runnable blocking operation that runs some code.
    * @param traceId an identifier that can be used to tell in a trace when an operation moves between threads.
    * @param <I> input value type to the function.
    * @return a stage that is completed after the action is done or throws an exception.
    */
   <I> CompletionStage<Void> thenRunBlocking(CompletionStage<? extends I> stage, Runnable runnable, Object traceId);

   /**
    * Replacement for {@code CompletionStage.thenApplyAsync()} that invokes the {@code Function} in a blocking thread
    * (if the current thread is non-blocking) or in the current thread (if the current thread is blocking).
    * The returned stage, if not complete, resumes any chained stage on the non-blocking executor.
    * <p>
    * Note that if the current thread is blocking, the task is invoked in the current thread meaning the stage is
    * always completed when returned, so any chained stage is also invoked on the current thread.
    * @param stage stage, that may or may not be complete, to apply.
    * @param function the blocking function.
    * @param traceId an identifier that can be used to tell in a trace when an operation moves between threads.
    * @param <I> input value type to the function.
    * @param <O> output value type after being transformed via function.
    * @return a stage that, when complete, contains the value returned from the function or a throwable.
    */
   <I, O> CompletionStage<O> thenApplyBlocking(CompletionStage<? extends I> stage,
         Function<? super I, ? extends O> function, Object traceId);

   /**
    * Replacement for {@code CompletionStage.thenComposeAsync()} that invokes the {@code Function} in a blocking thread
    * (if the current thread is non-blocking) or in the current thread (if the current thread is blocking and the stage
    * is completed).
    * The returned stage, if not complete, resumes any chained stage on the non-blocking executor.
    * <p>
    * Note that if the current thread is blocking and the stage is completed, the task is invoked in the current thread
    * meaning the stage is always completed when returned, so any chained stage is also invoked on the current thread.
    * <p>
    * Note this method is not normally required as the Function already returns a CompletionStage and it is recommended
    * to have the composed function just be non-blocking to begin with.
    * This method is here when invoking some method that may spuriously block to be safe.
    *
    * @param stage stage, that may or may not be complete, to compose.
    * @param function the blocking function.
    * @param traceId an identifier that can be used to tell in a trace when an operation moves between threads.
    * @param <I> input value type to the function.
    * @param <O> output value type after being transformed via function.
    * @return a stage that, when complete, contains the value returned from the composed function or a throwable.
    */
   <I, O> CompletionStage<O> thenComposeBlocking(CompletionStage<? extends I> stage,
         Function<? super I, ? extends CompletionStage<O>> function, Object traceId);

   /**
    * Replacement for {@code CompletionStage.whenCompleteAsync()} that invokes the {@code BiConsumer} in a blocking thread
    * (if the current thread is non-blocking) or in the current thread (if the current thread is blocking).
    * The returned stage, if not complete, resumes any chained stage on the non-blocking executor.
    * <p>
    * Note that if the current thread is blocking, the task is invoked in the current thread meaning the stage is
    * always completed when returned, so any chained stage is also invoked on the current thread.
    * @param stage stage, that may or may not be complete, to apply.
    * @param biConsumer the blocking biConsumer.
    * @param traceId an identifier that can be used to tell in a trace when an operation moves between threads.
    * @param <V> stage value type.
    * @return a stage that is complete when the biConsumer is complete, but retains the results from the original stage.
    */
   <V> CompletionStage<V> whenCompleteBlocking(CompletionStage<V> stage,
         BiConsumer<? super V, ? super Throwable> biConsumer, Object traceId);

   /**
    * When the provided stage is complete, continue the completion chain of the returned CompletionStage on the
    * supplied executor. If tracing is enabled, a trace message is printed using the object as an identifier to more
    * easily track the transition between threads.
    * <p>
    * This method is useful when an asynchronous computation completes and you do not want to run further processing
    * on the thread that returned it. An example may be that some blocking operation is performed on a special blocking
    * thread pool. However when the blocking operation completes we want to continue processing that result in a thread
    * pool that is for computational tasks.
    * <p>
    * If the supplied stage is already completed when invoking this command, it returns an already completed
    * stage, which means any additional dependent stages are run in the invoking thread.
    * @param <V> return value type of the supplied stage.
    * @param delay the stage to delay the continuation until complete.
    * @param traceId the identifier to print when tracing is enabled.
    * @return a CompletionStage that, when depended upon, runs any callback in the supplied executor.
    */
   <V> CompletionStage<V> continueOnNonBlockingThread(CompletionStage<V> delay, Object traceId);

   /**
    * Provided a publisher that is known to block when subscribed to. Thus if the thread that subscribes in a non
    * blocking thread we will instead subscribe on a blocking thread and observe on a non blocking thread for each
    * published value.
    * <p>
    * If, however, the subscribing thread is a blocking thread no threading changes will be done, which
    * means the publisher will be subscribed to on the invoking thread. In this case values have no guarantee as to
    * which thread they are observed on, dependent solely on how the Publisher publishes them.
    * @param publisher the publisher that, when subscribed to, blocks the current thread.
    * @param <V> the published entry types.
    * @return publisher that does not block the current thread.
    */
   <V> Publisher<V> blockingPublisher(Publisher<V> publisher);

   /**
    * Subscribes to the provided blocking publisher using the the blocking executor, ignoring all elements and returning
    * a {@link CompletionStage} with a value of null which completes on a non-blocking thread. This method is designed
    * to be used by a {@link Publisher} that when subscribed to has some type of side-effect that is blocking.
    * <p>
    * The returned {@link CompletionStage} will always be completed upon a non-blocking thread if the current thread is
    * non-blocking.
    * <p>
    * Note that if the current thread is blocking everything including subscription, publication and collection of
    * values will be done on the current thread.
    *
    * @param publisher the publisher that, when subscribed to, blocks the current thread.
    * @param <V>       the published entry types.
    * @return a completion stage that completes once the publisher has completed.
    */
   <V> CompletionStage<Void> blockingPublisherToVoidStage(Publisher<V> publisher, Object traceId);

   /**
    * Returns an executor that will run the given tasks on a blocking thread as required.
    * <p>
    * Note that this executor will always submit the task to the blocking thread pool, even if the requestor
    * is a blocking thread. This is different than other methods that will invoke the task in the invoking
    * thread if the invoking thread is blocking.
    * @return an executor that can run blocking commands.
    */
   Executor asExecutor(String name);

   /**
    * Provides a {@link BlockingExecutor} which is limited to the provided concurrency amount.
    * @param name name of the limited blocking executor.
    * @param concurrency maximum amount of concurrent operations to be performed via the returned executor.
    * @return a blocking executor limited in the amount of concurrent invocations.
    */
   BlockingExecutor limitedBlockingExecutor(String name, int concurrency);

   /**
    * Executor interface that submits task to a blocking pool that returns a stage that is guaranteed
    * to run any chained stages on a non-blocking thread if the stage is not yet complete.
    * <p>
    * Note that this executor runs the task in the invoking thread if the thread is a blocking thread.
    */
   interface BlockingExecutor {
      /**
       * Executes the given runnable on the blocking executor. The traceId is printed in the invoking thread, in the
       * blocking thread, and also during resumption of the non-blocking thread.
       * @param runnable blocking operation that runs some code.
       * @param traceId an identifier that can be used to tell in a trace when an operation moves between threads.
       * @return a stage that is completed after the runnable is done or throws an exception.
       */
      CompletionStage<Void> execute(Runnable runnable, Object traceId);

      /**
       * Executes the given supplier on the blocking executor. The traceId is printed in the invoking thread, in the
       * blocking thread, and also during resumption of the non-blocking thread.
       * @param supplier blocking operation that returns a value.
       * @param traceId an identifier that can be used to tell in a trace when an operation moves between threads.
       * @param <V> supplier type.
       * @return a stage that, when complete, contains the value returned from the supplier or a throwable.
       */
      <V> CompletionStage<V> supply(Supplier<V> supplier, Object traceId);
   }
}
