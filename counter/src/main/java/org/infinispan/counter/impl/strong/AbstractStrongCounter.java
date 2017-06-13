package org.infinispan.counter.impl.strong;

import static org.infinispan.counter.impl.entries.CounterValue.newCounterValue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import org.infinispan.AdvancedCache;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.CounterState;
import org.infinispan.counter.api.Handle;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.exception.CounterException;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.impl.function.AddFunction;
import org.infinispan.counter.impl.function.CompareAndSetFunction;
import org.infinispan.counter.impl.function.InitializeCounterFunction;
import org.infinispan.counter.impl.function.ReadFunction;
import org.infinispan.counter.impl.function.ResetFunction;
import org.infinispan.counter.impl.listener.CounterEventImpl;
import org.infinispan.counter.impl.listener.CounterFilterAndConvert;
import org.infinispan.counter.impl.listener.NotificationManager;
import org.infinispan.counter.logging.Log;
import org.infinispan.counter.util.Utils;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadOnlyMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;

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
@Listener(clustered = true, observation = Listener.Observation.POST, sync = true)
public abstract class AbstractStrongCounter implements StrongCounter {

   final StrongCounterKey key;
   private final FunctionalMap.ReadWriteMap<StrongCounterKey, CounterValue> readWriteMap;
   private final FunctionalMap.ReadOnlyMap<StrongCounterKey, CounterValue> readOnlyMap;
   private final NotificationManager notificationManager;
   private final CounterConfiguration configuration;
   private CounterValue weakCounter;

   AbstractStrongCounter(String counterName, AdvancedCache<StrongCounterKey, CounterValue> cache,
         CounterConfiguration configuration) {
      FunctionalMapImpl<StrongCounterKey, CounterValue> functionalMap = FunctionalMapImpl.create(cache)
            .withParams(Utils.getPersistenceMode(configuration.storage()));
      this.key = new StrongCounterKey(counterName);
      this.readWriteMap = ReadWriteMapImpl.create(functionalMap);
      this.readOnlyMap = ReadOnlyMapImpl.create(functionalMap);
      this.notificationManager = new NotificationManager();
      this.weakCounter = null;
      this.configuration = configuration;
      registerListener(cache);
   }

   public final void init() {
      try {
         CounterValue existingValue = readWriteMap.eval(key, new InitializeCounterFunction<>(configuration)).get();
         initCounterState(existingValue == null ? newCounterValue(configuration) : existingValue);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CounterException(e);
      } catch (ExecutionException e) {
         throw Utils.rethrowAsCounterException(e);
      }
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
      return readWriteMap.eval(key, new AddFunction<>(delta)).thenApply(this::handleAddResult);
   }

   @Override
   public final CompletableFuture<Void> reset() {
      return readWriteMap.eval(key, ResetFunction.getInstance());
   }

   @Override
   public final <T extends CounterListener> Handle<T> addListener(T listener) {
      return notificationManager.addListener(listener);
   }

   @Override
   public CompletableFuture<Boolean> compareAndSet(long expect, long update) {
      return readWriteMap.eval(key, new CompareAndSetFunction<>(expect, update)).thenApply(this::handleCASResult);
   }

   @CacheEntryModified
   public synchronized void updateState(CacheEntryEvent<StrongCounterKey, CounterValue> event) {
      CounterValue snapshot = event.getValue();
      notificationManager.notify(CounterEventImpl.create(weakCounter, snapshot));
      weakCounter = snapshot;
   }

   @Override
   public final CounterConfiguration getConfiguration() {
      return configuration;
   }

   protected abstract Boolean handleCASResult(CounterState state);

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
    * @return The {@link Log} to use.
    */
   protected abstract Log getLog();

   /**
    * Registers this instance as a cluster listener.
    * <p>
    * Note: It must be invoked when initialize the instance.
    */
   private void registerListener(AdvancedCache<StrongCounterKey, CounterValue> cache) {
      CounterFilterAndConvert<StrongCounterKey> filter = new CounterFilterAndConvert<>(key.getCounterName());
      cache.addListener(this, filter, filter);
   }

   /**
    * Initializes the weak value.
    *
    * @param value The initial value.
    */
   private synchronized void initCounterState(CounterValue value) {
      if (weakCounter == null) {
         weakCounter = value;
      }
   }

   /**
    * Retrieves and validate the value after a read.
    *
    * @param value The current value.
    * @return The current value.
    */
   private long handleReadResult(Long value) {
      if (value != null) {
         return value;
      }
      throw new CompletionException(getLog().counterDeleted());
   }
}
