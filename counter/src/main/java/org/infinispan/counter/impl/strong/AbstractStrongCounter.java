package org.infinispan.counter.impl.strong;

import static org.infinispan.counter.impl.Util.awaitCounterOperation;
import static org.infinispan.counter.impl.entries.CounterValue.newCounterValue;
import static org.infinispan.counter.util.Utils.getPersistenceMode;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterEvent;
import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.Handle;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.SyncStrongCounter;
import org.infinispan.counter.impl.SyncStrongCounterAdapter;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.impl.function.AddFunction;
import org.infinispan.counter.impl.function.CompareAndSwapFunction;
import org.infinispan.counter.impl.function.CreateAndAddFunction;
import org.infinispan.counter.impl.function.CreateAndCASFunction;
import org.infinispan.counter.impl.function.ReadFunction;
import org.infinispan.counter.impl.function.RemoveFunction;
import org.infinispan.counter.impl.function.ResetFunction;
import org.infinispan.counter.impl.listener.CounterEventGenerator;
import org.infinispan.counter.impl.listener.CounterEventImpl;
import org.infinispan.counter.impl.listener.CounterManagerNotificationManager;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadOnlyMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;

import net.jcip.annotations.GuardedBy;

/**
 * A base strong consistent counter implementation.
 * <p>
 * Implementation: The value is stored in a single key and it uses the Infinispan's concurrency control and distribution
 * to apply the write and reads. It uses the functional API.
 * <p>
 * Writes: The writes are performed by the functional API in order. The single key approach allows us to provide atomic
 * properties for the counter value.
 * <p>
 * Reads: The reads read the value from the cache and it can go remotely.
 * <p>
 * Weak Reads: This implementation supports weak cached reads. It uses clustered listeners to receive the notifications
 * of the actual value to store it locally.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public abstract class AbstractStrongCounter implements StrongCounter, CounterEventGenerator {

   final StrongCounterKey key;
   private final FunctionalMap.ReadWriteMap<StrongCounterKey, CounterValue> readWriteMap;
   private final FunctionalMap.ReadOnlyMap<StrongCounterKey, CounterValue> readOnlyMap;
   private final CounterManagerNotificationManager notificationManager;
   private final CounterConfiguration configuration;
   @GuardedBy("this")
   private CounterValue weakCounter;

   AbstractStrongCounter(String counterName, AdvancedCache<StrongCounterKey, CounterValue> cache,
         CounterConfiguration configuration, CounterManagerNotificationManager notificationManager) {
      this.notificationManager = notificationManager;
      FunctionalMapImpl<StrongCounterKey, CounterValue> functionalMap = FunctionalMapImpl.create(cache)
            .withParams(getPersistenceMode(configuration.storage()));
      this.key = new StrongCounterKey(counterName);
      this.readWriteMap = ReadWriteMapImpl.create(functionalMap);
      this.readOnlyMap = ReadOnlyMapImpl.create(functionalMap);
      this.weakCounter = null;
      this.configuration = configuration;
   }

   /**
    * It removes a strong counter from the {@code cache}, identified by the {@code counterName}.
    *
    * @param cache       The {@link Cache} to remove the counter from.
    * @param counterName The counter's name.
    */
   public static void removeStrongCounter(Cache<StrongCounterKey, CounterValue> cache, String counterName) {
      cache.remove(new StrongCounterKey(counterName));
   }

   public final void init() {
      registerListener();
      awaitCounterOperation(readOnlyMap.eval(key, ReadFunction.getInstance()).thenAccept(this::initCounterState));
   }

   @Override
   public final String getName() {
      return key.getCounterName().toString();
   }

   @Override
   public final CompletableFuture<Long> getValue() {
      return readOnlyMap.eval(key, ReadFunction.getInstance()).thenApply(this::handleReadResult);
   }

   @Override
   public final CompletableFuture<Long> addAndGet(long delta) {
      return readWriteMap.eval(key, new AddFunction<>(delta)).thenCompose(value -> checkAddResult(value, delta));
   }

   @Override
   public final CompletableFuture<Void> reset() {
      return readWriteMap.eval(key, ResetFunction.getInstance());
   }

   @Override
   public final <T extends CounterListener> Handle<T> addListener(T listener) {
      return notificationManager.registerUserListener(key.getCounterName(), listener);
   }

   @Override
   public CompletableFuture<Long> compareAndSwap(long expect, long update) {
      return readWriteMap.eval(key, new CompareAndSwapFunction<>(expect, update))
            .thenCompose(result -> checkCasResult(result, expect, update));
   }

   @Override
   public synchronized CounterEvent generate(CounterKey key, CounterValue value) {
      CounterValue newValue = value == null ?
            CounterValue.newCounterValue(configuration) :
            value;
      if (weakCounter == null || weakCounter.equals(newValue)) {
         weakCounter = newValue;
         return null;
      } else {
         CounterEvent event = CounterEventImpl.create(weakCounter, newValue);
         weakCounter = newValue;
         return event;
      }
   }

   public CompletableFuture<Void> remove() {
      return readWriteMap.eval(key, RemoveFunction.getInstance());
   }

   @Override
   public final CounterConfiguration getConfiguration() {
      return configuration;
   }

   @Override
   public SyncStrongCounter sync() {
      return new SyncStrongCounterAdapter(this);
   }

   public void destroyAndRemove() {
      removeListener();
      awaitCounterOperation(remove());
   }

   protected abstract Long handleCASResult(Object state);

   /**
    * Extracts and validates the value after a read.
    * <p>
    * Any exception should be thrown using {@link CompletionException}.
    *
    * @param counterValue The new {@link CounterValue}.
    * @return The new value stored in {@link CounterValue}.
    */
   protected abstract long handleAddResult(CounterValue counterValue);

   /**
    * Registers this instance as a cluster listener.
    * <p>
    * Note: It must be invoked when initialize the instance.
    */
   private void registerListener() {
      notificationManager.registerCounter(key.getCounterName(), this, null);
   }

   /**
    * Removes this counter from the notification manager.
    */
   private void removeListener() {
      notificationManager.removeCounter(key.getCounterName());
   }

   /**
    * Initializes the weak value.
    *
    * @param currentValue The current value.
    */
   private synchronized void initCounterState(Long currentValue) {
      if (weakCounter == null) {
         weakCounter = currentValue == null ?
               newCounterValue(configuration) :
               newCounterValue(currentValue, configuration);
      }
   }

   /**
    * Retrieves and validate the value after a read.
    *
    * @param value The current value.
    * @return The current value.
    */
   private long handleReadResult(Long value) {
      return value == null ?
            configuration.initialValue() :
            value;
   }

   private CompletableFuture<Long> checkAddResult(CounterValue value, long delta) {
      if (value == null) {
         //key doesn't exist in the cache. create and add.
         return readWriteMap.eval(key, new CreateAndAddFunction<>(configuration, delta))
               .thenApply(this::handleAddResult);
      } else {
         return CompletableFuture.completedFuture(handleAddResult(value));
      }
   }

   private CompletionStage<Long> checkCasResult(Object result, long expect, long update) {
      if (result == null) {
         //key doesn't exist in the cache. create and CAS
         return readWriteMap.eval(key, new CreateAndCASFunction<>(configuration, expect, update))
               .thenApply(this::handleCASResult);
      } else {
         return CompletableFuture.completedFuture(handleCASResult(result));
      }
   }
}
