package org.infinispan.util.concurrent;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * Orders multiple actions/tasks based on a key.
 * <p>
 * It has the following properties:
 * <ul>
 *    <li>If multiple actions have disjoint ordering keys, they are execute in parallel.</li>
 *    <li>If multiple actions have the same ordering keys, deadlocks are avoided between them.</li>
 *    <li>An action is only executed after the previous one is completed.</li>
 * </ul>
 *
 * @author Pedro Ruivo
 * @since 10.0
 */
public class ActionSequencer {

   private final Map<Object, SequenceEntry<?>> sequencer = new ConcurrentHashMap<>();
   private final LongAdder pendingActions = new LongAdder();
   private final ExecutorService executor;

   public ActionSequencer(ExecutorService executor) {
      this.executor = executor;
   }

   private static <T> CompletionStage<T> safeNonBlockingCall(Callable<? extends CompletionStage<T>> action) {
      try {
         return action.call();
      } catch (Exception e) {
         return CompletableFutures.completedExceptionFuture(e);
      }
   }

   /**
    * It order a non-blocking action.
    * <p>
    * It assumes the {@code action} does not block the invoked thread and it may execute it in this thread or, if there
    * is one or more pending actions, in a separate thread (provided by the {@code executor}).
    *
    * @param <T>    The return value type.
    * @param keys   The ordering keys.
    * @param action The {@link Callable} to invoke.
    * @return A {@link CompletableFuture} that is completed with the return value of resulting {@link
    * CompletableFuture}.
    * @throws NullPointerException if any of the parameter is null.
    */
   public <T> CompletionStage<T> orderOnKeys(Collection<?> keys, Callable<? extends CompletionStage<T>> action) {
      checkAction(action);
      Object[] dKeys = checkKeys(keys);
      if (dKeys.length == 0) {
         return safeNonBlockingCall(action);
      }
      SequenceEntry<T> entry;
      if (dKeys.length == 1) {
         entry = new SingleKeyNonBlockingSequenceEntry<>(action, executor, dKeys[0]);
      } else {
         entry = new MultiKeyNonBlockingSequenceEntry<>(action, executor, dKeys);
      }

      registerAction(entry);
      return entry;
   }

   public <T> CompletionStage<T> orderOnKey(Object key, Callable<? extends CompletionStage<T>> action) {
      checkAction(action);
      SequenceEntry<T> entry = new SingleKeyNonBlockingSequenceEntry<>(action, executor, checkKey(key));
      registerAction(entry);
      return entry;
   }

   public long getPendingActions() {
      return pendingActions.longValue();
   }

   public int getMapSize() {
      return sequencer.size();
   }

   private <T> void registerAction(SequenceEntry<T> entry) {
      pendingActions.increment();
      entry.register();
   }

   private void checkAction(Callable<?> action) {
      Objects.requireNonNull(action, "Action cannot be null.");
   }

   private Object[] checkKeys(Collection<?> keys) {
      return Objects.requireNonNull(keys, "Keys cannot be null.")
            .stream().filter(Objects::nonNull).distinct().toArray();
   }

   private Object checkKey(Object key) {
      return Objects.requireNonNull(key, "Key cannot be null.");
   }

   private void remove(Object key, SequenceEntry<?> entry) {
      sequencer.remove(key, entry);
      pendingActions.decrement();
   }

   private void remove(Object[] keys, SequenceEntry<?> entry) {
      for (Object key : keys) {
         sequencer.remove(key, entry);
      }
      pendingActions.decrement();
   }

   private abstract static class SequenceEntry<T> extends CompletableFuture<T>
         implements BiFunction<Object, Throwable, Void>, //for handleAsync (to chain on the previous entry)
         BiConsumer<T, Throwable> { //for whenComplete (to chain on the action result)

      final Callable<? extends CompletionStage<T>> action;
      final ExecutorService executor;

      SequenceEntry(Callable<? extends CompletionStage<T>> action, ExecutorService executor) {
         this.action = action;
         this.executor = executor;
      }

      public void register() {
         CompletionStage<?> previousStage = putInMap();
         if (previousStage != null) {
            previousStage.handleAsync(this, executor);
         } else {
            apply(null, null);
         }
      }

      @Override
      public void accept(T o, Throwable throwable) {
         removeFromMap();

         if (throwable == null) {
            complete(o);
         } else {
            completeExceptionally(throwable);
         }
      }

      @Override
      public final Void apply(Object o, Throwable t) {
         CompletionStage<T> cf = safeNonBlockingCall(action);
         cf.whenComplete(this);
         return null;
      }

      /**
       * Register the current entry in the sequencer map for all the affected keys.
       *
       * @return A stage that completes when all the previous entries have completed
       */
      abstract CompletionStage<?> putInMap();

      /**
       * Remove this entry from the sequencer map for all the affected keys, unless it was already replaced by another
       * entry.
       */
      abstract void removeFromMap();

   }


   private class SingleKeyNonBlockingSequenceEntry<T> extends SequenceEntry<T> {
      private final Object key;

      SingleKeyNonBlockingSequenceEntry(Callable<? extends CompletionStage<T>> action, ExecutorService executor,
            Object key) {
         super(action, executor);
         this.key = key;
      }

      @Override
      public CompletionStage<?> putInMap() {
         return sequencer.put(key, this);
      }

      @Override
      public void removeFromMap() {
         remove(key, this);
      }
   }

   private class MultiKeyNonBlockingSequenceEntry<T> extends SequenceEntry<T> {

      private final Object[] keys;

      MultiKeyNonBlockingSequenceEntry(Callable<? extends CompletionStage<T>> action, ExecutorService executor,
            Object[] keys) {
         super(action, executor);
         this.keys = keys;
      }

      @Override
      public CompletionStage<?> putInMap() {
         AggregateCompletionStage<?> previousCF = CompletionStages.aggregateCompletionStage();
         synchronized (ActionSequencer.this) {
            BiFunction<Object, SequenceEntry<?>, SequenceEntry<?>> mapping = (key, previousEntry) -> waitFromPrevious(
                  previousEntry, previousCF);
            for (Object key : keys) {
               sequencer.compute(key, mapping);
            }
         }
         return previousCF.freeze();
      }

      @Override
      void removeFromMap() {
         remove(keys, this);
      }

      SequenceEntry<?> waitFromPrevious(SequenceEntry<?> previousEntry, AggregateCompletionStage<?> previousCF) {
         if (previousEntry != null) {
            previousCF.dependsOn(previousEntry);
         }
         return this;
      }
   }

}
