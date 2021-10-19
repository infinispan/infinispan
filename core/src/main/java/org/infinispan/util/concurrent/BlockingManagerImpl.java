package org.infinispan.util.concurrent;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import org.infinispan.commons.executors.BlockingResource;
import org.infinispan.executors.LimitedExecutor;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.schedulers.Schedulers;


@Scope(Scopes.GLOBAL)
public class BlockingManagerImpl implements BlockingManager {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private final AtomicInteger id = log.isTraceEnabled() ? new AtomicInteger() : null;

   @Inject @ComponentName(KnownComponentNames.NON_BLOCKING_EXECUTOR)
   Executor nonBlockingExecutor;
   // This should eventually be the only reference to blocking executor
   @Inject @ComponentName(KnownComponentNames.BLOCKING_EXECUTOR)
   Executor blockingExecutor;

   private Scheduler blockingScheduler;
   private Scheduler nonBlockingScheduler;

   @Start
   void start() {
      blockingScheduler = Schedulers.from(new ReentrantBlockingExecutor());
      nonBlockingScheduler = Schedulers.from(nonBlockingExecutor);
   }

   private String nextTraceId() {
      return id != null ? "-BlockingManagerImpl-" + id.getAndIncrement() : null;
   }

   private class ReentrantBlockingExecutor implements Executor {
      @Override
      public void execute(Runnable command) {
         runBlockingOperation(command, nextTraceId(), blockingExecutor, false);
      }
   }

   @Override
   public CompletionStage<Void> runBlocking(Runnable runnable, Object traceId) {
      return runBlockingOperation(runnable, traceId, blockingExecutor);
   }

   @Override
   public <E> CompletionStage<Void> subscribeBlockingConsumer(Publisher<E> publisher, Consumer<E> consumer,
         Object traceId) {
      Flowable<E> valuePublisher = Flowable.fromPublisher(publisher)
            .observeOn(blockingScheduler);

      if (log.isTraceEnabled()) {
         valuePublisher = valuePublisher.doOnNext(value -> log.tracef("Invoking blocking consumer for %s with value %s", traceId, value));
      }
      return continueOnNonBlockingThread(valuePublisher
            .doOnNext(consumer::accept)
            .ignoreElements()
            .toCompletionStage(null), traceId);
   }

   @Override
   public <T, A, R> CompletionStage<R> subscribeBlockingCollector(Publisher<T> publisher, Collector<? super T, A, R> collector,
         Object traceId) {
      Flowable<T> valuePublisher = Flowable.fromPublisher(publisher)
            .observeOn(blockingScheduler);

      if (log.isTraceEnabled()) {
         valuePublisher = valuePublisher.doOnNext(value -> log.tracef("Invoking blocking collector for %s with value %s", traceId, value));
      }
      return continueOnNonBlockingThread(Flowable.fromPublisher(valuePublisher)
            // Unfortunately rxjava doesn't have the generics as they should :(
            .collect((Collector<T, A, R>) collector)
            .toCompletionStage(), traceId);
   }

   private CompletionStage<Void> runBlockingOperation(Runnable runnable, Object traceId, Executor executor) {
      return runBlockingOperation(runnable, traceId, executor, true);
   }

   private CompletionStage<Void> runBlockingOperation(Runnable runnable, Object traceId, Executor executor,
      boolean requireReturnOnNonBlockingThread) {
      if (isCurrentThreadBlocking()) {
         if (log.isTraceEnabled()) {
            log.tracef("Invoked run on a blocking thread, running %s in same blocking thread", traceId);
         }
         try {
            runnable.run();
            return CompletableFutures.completedNull();
         } catch (Throwable t) {
            return CompletableFutures.completedExceptionFuture(t);
         }
      }
      CompletionStage<Void> stage;
      if (log.isTraceEnabled()) {
         log.tracef("Submitting blocking run operation %s to blocking thread", traceId);
         stage = CompletableFuture.runAsync(() -> {
            log.tracef("Running blocking run operation %s", traceId);
            runnable.run();
         }, executor);
      } else {
         stage = CompletableFuture.runAsync(runnable, executor);
      }
      return requireReturnOnNonBlockingThread ? continueOnNonBlockingThread(stage, traceId) : stage;
   }

   @Override
   public <V> CompletionStage<V> supplyBlocking(Supplier<V> supplier, Object traceId) {
      return supplyBlockingOperation(supplier, traceId, blockingExecutor);
   }

   private <V> CompletionStage<V> supplyBlockingOperation(Supplier<V> supplier, Object traceId, Executor executor) {
      if (isCurrentThreadBlocking()) {
         if (log.isTraceEnabled()) {
            log.tracef("Invoked supply on a blocking thread, running %s in same blocking thread", traceId);
         }
         try {
            return CompletableFuture.completedFuture(supplier.get());
         } catch (Throwable t) {
            return CompletableFutures.completedExceptionFuture(t);
         }
      }
      CompletionStage<V> stage;
      if (log.isTraceEnabled()) {
         log.tracef("Submitting blocking supply operation %s to blocking thread", traceId);
         stage = CompletableFuture.supplyAsync(() -> {
            log.tracef("Running blocking supply operation %s", traceId);
            return supplier.get();
         }, executor);
      } else {
         stage = CompletableFuture.supplyAsync(supplier, executor);
      }
      return continueOnNonBlockingThread(stage, traceId);
   }

   @Override
   public <I, O> CompletionStage<O> handleBlocking(CompletionStage<? extends I> stage,
         BiFunction<? super I, Throwable, ? extends O> function, Object traceId) {
      if (isCurrentThreadBlocking()) {
         I value = null;
         Throwable throwable = null;
         try {
            if (log.isTraceEnabled()) {
               log.tracef("Invoked handle on a blocking thread, joining %s in same blocking thread", traceId);
            }
            value = CompletionStages.join(stage);
         } catch (Throwable t) {
            throwable = t;
         }
         return CompletableFuture.completedFuture(function.apply(value, throwable));
      }
      return continueOnNonBlockingThread(stage.handleAsync(function, blockingExecutor), traceId);
   }

   @Override
   public <I, O> CompletionStage<O> thenApplyBlocking(CompletionStage<? extends I> stage,
         Function<? super I, ? extends O> function, Object traceId) {
      if (isCurrentThreadBlocking()) {
         if (log.isTraceEnabled()) {
            log.tracef("Invoked thenApply on a blocking thread, joining %s in same blocking thread", traceId);
         }
         try {
            I value = CompletionStages.join(stage);
            return CompletableFuture.completedFuture(function.apply(value));
         } catch (Throwable t) {
            return CompletableFutures.completedExceptionFuture(t);
         }
      }
      return continueOnNonBlockingThread(stage.thenApplyAsync(function, blockingExecutor), traceId);
   }

   @Override
   public <I> CompletionStage<Void> thenRunBlocking(CompletionStage<? extends I> stage, Runnable runnable, Object traceId) {
      if (isCurrentThreadBlocking()) {
         if (log.isTraceEnabled()) {
            log.tracef("Invoked thenRun on a blocking thread, joining %s in same blocking thread", traceId);
         }
         try {
            CompletionStages.join(stage);
            runnable.run();
            return CompletableFutures.completedNull();
         } catch (Throwable t) {
            return CompletableFutures.completedExceptionFuture(t);
         }
      }
      return continueOnNonBlockingThread(stage.thenRunAsync(runnable, blockingExecutor), traceId);
   }

   @Override
   public <V> CompletionStage<V> whenCompleteBlocking(CompletionStage<V> stage,
         BiConsumer<? super V, ? super Throwable> biConsumer, Object traceId) {
      if (isCurrentThreadBlocking()) {
         if (log.isTraceEnabled()) {
            log.tracef("Invoked whenComplete on a blocking thread, joining %s in same blocking thread", traceId);
         }
         V value = null;
         Throwable throwable = null;
         try {
            value = CompletionStages.join(stage);
         } catch (Throwable t) {
            throwable = t;
         }
         try {
            biConsumer.accept(value, throwable);
         } catch (Throwable t) {
            if (throwable == null) {
               return CompletableFutures.completedExceptionFuture(t);
            }
            throwable.addSuppressed(t);
            return CompletableFutures.completedExceptionFuture(throwable);
         }
         return stage.whenComplete(biConsumer);
      }
      return continueOnNonBlockingThread(stage.whenCompleteAsync(biConsumer, blockingExecutor), traceId);
   }

   @Override
   public <V> CompletionStage<V> continueOnNonBlockingThread(CompletionStage<V> delay, Object traceId) {
      if (CompletionStages.isCompletedSuccessfully(delay)) {
         if (log.isTraceEnabled()) {
            log.tracef("Stage for %s was already completed, returning in same thread", traceId);
         }
         return delay;
      }
      return delay.whenCompleteAsync((v, t) -> {
         if (t != null) {
            if (log.isTraceEnabled()) {
               log.tracef("Continuing execution of id %s with exception %s", traceId, t.getMessage());
            }
         } else if (log.isTraceEnabled()) {
            log.tracef("Continuing execution of id %s", traceId);
         }
      }, nonBlockingExecutor);
   }

   @Override
   public <V> Publisher<V> blockingPublisher(Publisher<V> publisher) {
      return Flowable.defer(() -> {
         if (isCurrentThreadBlocking()) {
            return publisher;
         }
         if (log.isTraceEnabled()) {
            int publisherId = System.identityHashCode(publisher);
            log.tracef("Blocking publisher start %d", publisherId);
            return Flowable.fromPublisher(publisher)
                           .subscribeOn(blockingScheduler)
                           .observeOn(nonBlockingScheduler)
                           .doFinally(() -> log.tracef("Blocking publisher done %d", publisherId));
         }
         return Flowable.fromPublisher(publisher)
                        .subscribeOn(blockingScheduler)
                        .observeOn(nonBlockingScheduler);
      });
   }

   public <V> CompletionStage<Void> blockingPublisherToVoidStage(Publisher<V> publisher, Object traceId) {
      Flowable<V> flowable = Flowable.fromPublisher(publisher);
      if (!isCurrentThreadBlocking()) {
         if (log.isTraceEnabled()) {
            flowable = flowable.doOnSubscribe(subscription -> log.tracef("Subscribing to %s on blocking thread", traceId));
         }
         flowable = flowable.subscribeOn(blockingScheduler);
         if (log.isTraceEnabled()) {
            flowable = flowable.doOnSubscribe(subscription -> log.tracef("Publisher %s subscribing thread is %s", traceId, Thread.currentThread()));
         }
      } else if (log.isTraceEnabled()) {
         log.tracef("Invoked on a blocking thread, subscribing %s in same blocking thread", traceId);
      }

      CompletionStage<Void> stage = flowable
            .ignoreElements()
            .toCompletionStage(null);

      return continueOnNonBlockingThread(stage, traceId);
   }

   @Override
   public Executor asExecutor(String name) {
      if (!log.isTraceEnabled()) {
         return blockingExecutor;
      }

      return task -> {
         log.tracef("Submitting blocking run operation %s with name %s to blocking thread", task, name);
         blockingExecutor.execute(() -> {
            log.tracef("Running blocking operation %s with name %s on blocking thread", task, name);
            task.run();
         });
      };
   }

   @Override
   public BlockingExecutor limitedBlockingExecutor(String name, int concurrentExecutions) {
      LimitedExecutor limitedExecutor = new LimitedExecutor(name, blockingExecutor, concurrentExecutions);
      return new LimitedBlockingExecutor(limitedExecutor);
   }

   private class LimitedBlockingExecutor implements BlockingExecutor {
      private final LimitedExecutor limitedExecutor;

      private LimitedBlockingExecutor(LimitedExecutor limitedExecutor) {
         this.limitedExecutor = limitedExecutor;
      }

      @Override
      public CompletionStage<Void> execute(Runnable runnable, Object traceId) {
         return runBlockingOperation(runnable, traceId, limitedExecutor);
      }

      @Override
      public <V> CompletionStage<V> supply(Supplier<V> supplier, Object traceId) {
         return supplyBlockingOperation(supplier, traceId, limitedExecutor);
      }
   }

   // This method is designed to be overridden for testing purposes
   protected boolean isCurrentThreadBlocking() {
      return Thread.currentThread().getThreadGroup() instanceof BlockingResource;
   }
}
