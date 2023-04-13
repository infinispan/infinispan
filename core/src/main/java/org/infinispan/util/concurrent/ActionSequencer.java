package org.infinispan.util.concurrent;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import org.infinispan.commons.stat.DefaultSimpleStat;
import org.infinispan.commons.stat.SimpleStat;
import org.infinispan.commons.time.TimeService;

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
   private static final StatCollector NO_STATS = new StatCollector();
   private final Map<Object, SequenceEntry<?>> sequencer = new ConcurrentHashMap<>();
   private final LongAdder pendingActions = new LongAdder();
   private final LongAdder runningActions = new LongAdder();
   private final TimeService timeService;
   private final Executor executor;
   private final boolean forceExecutor;
   private final SimpleStat queueTimes = new DefaultSimpleStat();
   private final SimpleStat runningTimes = new DefaultSimpleStat();
   private volatile boolean collectStats;

   /**
    * @param executor      Executor to run submitted actions.
    * @param forceExecutor If {@code false}, run submitted actions on the submitter thread if possible. If {@code true},
    *                      always run submitted actions on the executor.
    */
   public ActionSequencer(Executor executor, boolean forceExecutor, TimeService timeService) {
      this.executor = executor;
      this.forceExecutor = forceExecutor;
      this.timeService = timeService;
   }

   private static <T> CompletionStage<T> safeNonBlockingCall(Callable<? extends CompletionStage<T>> action) {
      try {
         return action.call();
      } catch (Exception e) {
         return CompletableFuture.failedFuture(e);
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
      StatCollector statCollector = newStatCollector();
      SequenceEntry<T> entry;
      if (dKeys.length == 1) {
         entry = new SingleKeyNonBlockingSequenceEntry<>(action, dKeys[0], statCollector);
      } else {
         entry = new MultiKeyNonBlockingSequenceEntry<>(action, dKeys, statCollector);
      }

      registerAction(entry);
      return entry;
   }

   public <T> CompletionStage<T> orderOnKey(Object key, Callable<? extends CompletionStage<T>> action) {
      checkAction(action);
      StatCollector statCollector = newStatCollector();
      SequenceEntry<T> entry = new SingleKeyNonBlockingSequenceEntry<>(action, checkKey(key), statCollector);
      registerAction(entry);
      return entry;
   }

   public long getPendingActions() {
      return pendingActions.longValue();
   }

   public long getRunningActions() {
      return runningActions.longValue();
   }

   public void resetStatistics() {
      runningTimes.reset();
      queueTimes.reset();
   }

   public long getAverageQueueTimeNanos() {
      return queueTimes.getAverage(-1);
   }

   public long getAverageRunningTimeNanos() {
      return runningTimes.getAverage(-1);
   }

   public void setStatisticEnabled(boolean enable) {
      collectStats = enable;
      if (!enable) {
         resetStatistics();
      }
   }

   public int getMapSize() {
      return sequencer.size();
   }

   private <T> void registerAction(SequenceEntry<T> entry) {
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
   }

   private void remove(Object[] keys, SequenceEntry<?> entry) {
      for (Object key : keys) {
         sequencer.remove(key, entry);
      }
   }

   private StatCollector newStatCollector() {
      return collectStats ? new StatEnabledCollector() : NO_STATS;
   }

   private static class StatCollector {
      void taskCreated() {

      }

      void taskStarted() {

      }

      void taskFinished() {

      }
   }

   private abstract class SequenceEntry<T> extends CompletableFuture<T>
         implements BiFunction<Object, Throwable, Void>, //for handleAsync (to chain on the previous entry)
         BiConsumer<T, Throwable>, //for whenComplete (to chain on the action result)
         Runnable { //executes the actions

      final Callable<? extends CompletionStage<T>> action;
      final StatCollector statCollector;

      SequenceEntry(Callable<? extends CompletionStage<T>> action, StatCollector statCollector) {
         this.action = action;
         this.statCollector = statCollector;
      }

      public void register() {
         statCollector.taskCreated();
         CompletionStage<?> previousStage = putInMap();
         if (previousStage != null) {
            previousStage.handleAsync(this, executor);
         } else if (forceExecutor) {
            //execute the action in another thread.
            executor.execute(this);
         } else {
            run();
         }
      }

      @Override
      public final void accept(T o, Throwable throwable) {
         removeFromMap();
         statCollector.taskFinished();

         if (throwable == null) {
            complete(o);
         } else {
            completeExceptionally(throwable);
         }
      }

      @Override
      public final Void apply(Object o, Throwable t) {
         run();
         return null;
      }

      @Override
      public final void run() {
         statCollector.taskStarted();
         CompletionStage<T> cf = safeNonBlockingCall(action);
         cf.whenComplete(this);
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

      SingleKeyNonBlockingSequenceEntry(Callable<? extends CompletionStage<T>> action, Object key,
            StatCollector statCollector) {
         super(action, statCollector);
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

      MultiKeyNonBlockingSequenceEntry(Callable<? extends CompletionStage<T>> action, Object[] keys,
            StatCollector statCollector) {
         super(action, statCollector);
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

   private class StatEnabledCollector extends StatCollector {

      private volatile long createdTimestamp = -1;
      private volatile long startedTimestamp = -1;

      @Override
      void taskCreated() {
         pendingActions.increment();
         createdTimestamp = timeService.time();
      }

      @Override
      void taskStarted() {
         runningActions.increment();
         startedTimestamp = timeService.time();
      }

      @Override
      void taskFinished() {
         runningActions.decrement();
         pendingActions.decrement();
         long endTimestamp = timeService.time();
         queueTimes.record(startedTimestamp - createdTimestamp);
         runningTimes.record(endTimestamp - startedTimestamp);
      }
   }

}
