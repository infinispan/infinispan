package org.infinispan.util.concurrent;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiConsumer;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Utility methods for handling {@link CompletionStage} instances.
 * @author wburns
 * @since 10.0
 */
public class CompletionStages {
   private CompletionStages() { }

   private static final Log log = LogFactory.getLog(CompletionStages.class);
   private static final boolean trace = log.isTraceEnabled();

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
    * @param continuationExecutor the executor to run any further completion chain methods on
    * @param traceId the id to print when tracing is enabled
    * @return a CompletionStage that when depended upon will run any callback in the supplied executor
    */
   public static <V> CompletionStage<V> continueOnExecutor(CompletionStage<V> delay,
                                                           Executor continuationExecutor, Object traceId) {
      if (isCompletedSuccessfully(delay)) {
         if (trace) {
            log.tracef("Stage for %s was already completed, returning in same thread", traceId);
         }
         return delay;
      }
      return delay.whenCompleteAsync((v, t) -> {
         if (t != null) {
            if (trace) {
               log.tracef("Continuing execution of id %s with exception %s", traceId, t.getMessage());
            }
         } else if (trace) {
            log.tracef("Continuing execution of id %s", traceId);
         }
      }, continuationExecutor);
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

   /**
    * Abstract {@link AggregateCompletionStage} that will keep a count of non completed stages it depends upon while
    * only registering to be notified when each completes, decrementing the counter. The returned CompletionStage
    * via {@link #freeze()} will be completed when the counter is zero, providing the value returned from
    * {@link #getValue()} as the result.
    * This class implements BiConsumer and extends CompletableFuture to avoid additional object/lambda allocation per instance
    * @param <R>
    */
   private static abstract class AbstractAggregateCompletionStage<R> extends CompletableFuture<R>
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
      final public AggregateCompletionStage<R> dependsOn(CompletionStage<?> stage) {
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
      final public CompletionStage<R> freeze() {
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
}
