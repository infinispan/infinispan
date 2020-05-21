package org.infinispan.util.concurrent;

import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;

/**
 * Runs tasks on the blocking executor and returns a {@code CompletionStage} or {@code Publisher} that continues on the
 * non-blocking executor, similar to {@code stage.handleAsync(callback, blockingExecutor).whenCompleteAsync(NOOP, nonBlockingExecutor)}.
 * <p>
 * If the current thread is blocking, it blocks until the task may run, runs the task in the current thread, and returns a
 * completed {@code CompletionStage}, so it <em>does not</em> continue the execution on the non-blocking executor.
 * <p>
 * Many of the methods on {@code BlockingManager} allow an id to be passed when performing the operation. This id will
 * be printed to the TRACE log. It is therefore advised to provide something unique so that
 * if a log is needed it will be easier to track the stream of operations across threads.
 */
public interface BlockingManager {
   /**
    * Replacement for {@code CompletionStage.runAsync()} that invokes the {@code Runnable} in a blocking thread
    * (if the current thread is non-blocking) or in the current thread (if the current thread is blocking).
    * The returned stage if not complete will resume any chained stage on the non blocking executor.
    * <p>
    * Note that if the current thread is blocking, the task is invoked in the current thread meaning the stage will
    * always be completed when returned, so any chained stage will also be invoked on the current thread.
    * @param runnable blocking operation that runs something
    * @param traceId an identifier that can be used to tell in a trace when an operation moves between threads
    * @return a stage that is completed after the runnable is done or throws an exception
    */
   CompletionStage<Void> runBlocking(Runnable runnable, Object traceId);

   /**
    * Replacement for {@code CompletionStage.supplyAsync()} that invokes the {@code Supplier} in a blocking thread
    * (if the current thread is non-blocking) or in the current thread (if the current thread is blocking).
    * The returned stage if not complete will resume any chained stage on the non blocking executor.
    * <p>
    * Note that if the current thread is blocking, the task is invoked in the current thread meaning the stage will
    * always be completed when returned, so any chained stage will also be invoked on the current thread.
    * @param <V> the supplied type
    * @param supplier blocking operation that returns a value
    * @param traceId an identifier that can be used to tell in a trace when an operation moves between threads
    * @return a stage that when complete will contain the value returned from the supplier or a throwable
    */
   <V> CompletionStage<V> supplyBlocking(Supplier<V> supplier, Object traceId);

   /**
    * Replacement for {@code CompletionStage.handleAsync()} that invokes the {@code BiFunction} in a blocking thread
    * (if the current thread is non-blocking) or in the current thread (if the current thread is blocking).
    * The returned stage if not complete will resume any chained stage on the non blocking executor.
    * <p>
    * Note that if the current thread is blocking, the task is invoked in the current thread meaning the stage will
    * always be completed when returned, so any chained stage will also be invoked on the current thread.
    * @param stage stage that may or may not be complete to handle
    * @param function the blocking function
    * @param traceId an identifier that can be used to tell in a trace when an operation moves between threads
    * @param <I> input value type to the function
    * @param <O> output value type after being transformed via function
    * @return a stage that when complete will contain the value returned from the function or a throwable
    */
   <I, O> CompletionStage<O> handleBlocking(CompletionStage<? extends I> stage,
         BiFunction<? super I, Throwable, ? extends O> function, Object traceId);

   /**
    * Replacement for {@code CompletionStage.thenApplyAsync()} that invokes the {@code Function} in a blocking thread
    * (if the current thread is non-blocking) or in the current thread (if the current thread is blocking).
    * The returned stage if not complete will resume any chained stage on the non blocking executor.
    * <p>
    * Note that if the current thread is blocking, the task is invoked in the current thread meaning the stage will
    * always be completed when returned, so any chained stage will also be invoked on the current thread.
    * @param stage stage that may or may not be complete to apply
    * @param function the blocking function
    * @param traceId an identifier that can be used to tell in a trace when an operation moves between threads
    * @param <I> input value type to the function
    * @param <O> output value type after being transformed via function
    * @return a stage that when complete will contain the value returned from the function or a throwable
    */
   <I, O> CompletionStage<O> thenApplyBlocking(CompletionStage<? extends I> stage,
         Function<? super I, ? extends O> function, Object traceId);

   /**
    * Replacement for {@code CompletionStage.whenCompleteAsync()} that invokes the {@code BiConsumer} in a blocking thread
    * (if the current thread is non-blocking) or in the current thread (if the current thread is blocking).
    * The returned stage if not complete will resume any chained stage on the non blocking executor.
    * <p>
    * Note that if the current thread is blocking, the task is invoked in the current thread meaning the stage will
    * always be completed when returned, so any chained stage will also be invoked on the current thread.
    * @param stage stage that may or may not be complete to apply
    * @param biConsumer the blocking biConsumer
    * @param traceId an identifier that can be used to tell in a trace when an operation moves between threads
    * @param <V> stage value type
    * @return a stage that is complete when the biConsumer is but retains the results from the original stage
    */
   <V> CompletionStage<V> whenCompleteBlocking(CompletionStage<V> stage,
         BiConsumer<? super V, ? super Throwable> biConsumer, Object traceId);

   /**
    * When the provided stage is complete, continue the completion chain of the returned CompletionStage on the
    * supplied executor. If tracing is enabled a trace message is printed using the object as an identifier to more
    * easily track the transition between threads.
    * <p>
    * This method is useful when an asynchronous computation completes and you do not want to run further processing
    * on the thread that returned it. An example may be that some blocking operation is performed on a special blocking
    * thread pool. However when the blocking operation completes we will want to continue the processing of that result
    * in a thread pool that is for computational tasks.
    * <p>
    * If the supplied stage is already completed when invoking this command, this will return an already completed
    * stage, which means any additional dependent stages will run in the invoking thread.
    * @param <V> return value type of the supplied stage
    * @param delay the stage to delay the continuation until complete
    * @param traceId the id to print when tracing is enabled
    * @return a CompletionStage that when depended upon will run any callback in the supplied executor
    */
   <V> CompletionStage<V> continueOnNonBlockingThread(CompletionStage<V> delay, Object traceId);

   /**
    * Provided a publisher that is known to block when subscribed to, this will ensure that the publisher is subscribed
    * on the blocking executor and any values published will be observed on a non blocking thread. Note that if a
    * blocking thread subscribes to the publisher these additional threads will not be used and thus the entire Publisher
    * is subscribed and observed on the invoking thread.
    * @param publisher the publisher that when subscribed to will block
    * @param <V> the published entry types
    * @return publisher that will not block the current thread
    */
   <V> Publisher<V> blockingPublisher(Publisher<V> publisher);

   /**
    * Provides a {@link BlockingExecutor} which is limited to the provided concurrency amount.
    * @param name name of the limited blocking executor
    * @param concurrency maximum amount of concurrent operations to be performed via the returned executor
    * @return a blocking executor limited in the amount of concurrent invocations
    */
   BlockingExecutor limitedBlockingExecutor(String name, int concurrency);

   /**
    * Executor interface used to submit tasks to a blocking pool that will return a stage that will
    * be guaranteed to run any chained stages on a non blocking thread if the stage is not yet complete.
    * <p>
    * Note that this executor will run the task in the invoking thread if the thread is a blocking thread.
    */
   interface BlockingExecutor {
      /**
       * Executes the given runnable on the blocking executor. The traceId is printed in the invoking thread, in the
       * blocking thread and also during resumption of the non blocking thread.
       * @param runnable blocking operation that runs something
       * @param traceId an identifier that can be used to tell in a trace when an operation moves between threads
       * @return a stage that is completed after the runnable is done or throws an exception
       */
      CompletionStage<Void> execute(Runnable runnable, Object traceId);

      /**
       * Executes the given supplier on the blocking executor. The traceId is printed in the invoking thread, in the
       * blocking thread and also during resumption of the non blocking thread.
       * @param supplier blocking operation that returns a value
       * @param traceId an identifier that can be used to tell in a trace when an operation moves between threads
       * @param <V> supplier type
       * @return a stage that when complete will contain the value returned from the supplier or a throwable
       */
      <V> CompletionStage<V> supply(Supplier<V> supplier, Object traceId);
   }
}
