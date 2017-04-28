package org.infinispan.counter.impl.manager;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.counter.impl.strong.BoundedStrongCounter;
import org.infinispan.counter.impl.strong.UnboundedStrongCounter;
import org.infinispan.counter.impl.weak.WeakCounterImpl;
import org.infinispan.counter.logging.Log;
import org.infinispan.counter.util.Utils;

/**
 * A {@link CounterManager} implementation for embedded cache manager.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class EmbeddedCounterManager implements CounterManager {
   private static final long WAIT_CACHES_TIMEOUT = TimeUnit.SECONDS.toNanos(15);
   private static final Log log = LogFactory.getLog(EmbeddedCounterManager.class, Log.class);

   private final Map<String, Object> counters;
   private final CompletableFuture<CacheHolder> future;
   private final boolean allowPersistence;

   public EmbeddedCounterManager(CompletableFuture<CacheHolder> future, boolean allowPersistence) {
      this.allowPersistence = allowPersistence;
      this.counters = new ConcurrentHashMap<>();
      this.future = future;
   }

   private static <T> T validateCounter(Class<T> tClass, Object retVal) {
      Class<?> rClass = retVal.getClass();
      if (tClass.isAssignableFrom(rClass)) {
         return tClass.cast(retVal);
      }
      throw log.invalidCounterType(tClass.getSimpleName(), rClass.getSimpleName());
   }

   private static CacheHolder extractCacheHolder(CompletableFuture<CacheHolder> future) {
      try {
         return future.get(WAIT_CACHES_TIMEOUT, TimeUnit.NANOSECONDS);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         log.interruptedWhileWaitingForCaches();
         return null;
      } catch (ExecutionException | TimeoutException e) {
         log.exceptionWhileWaitingForCached(e);
         return null;
      }
   }

   private static WeakCounter createWeakCounter(String counterName, CounterConfiguration configuration,
         CacheHolder holder) {
      WeakCounterImpl counter = new WeakCounterImpl(counterName, holder.getCounterCache(configuration), configuration);
      counter.init();
      return counter;
   }

   private static StrongCounter createBoundedStrongCounter(String counterName, CounterConfiguration configuration,
         CacheHolder holder) {
      BoundedStrongCounter counter = new BoundedStrongCounter(counterName, holder.getCounterCache(configuration),
            configuration);
      counter.init();
      return counter;
   }

   private static StrongCounter createUnboundedStrongCounter(String counterName, CounterConfiguration configuration,
         CacheHolder holder) {
      UnboundedStrongCounter counter = new UnboundedStrongCounter(counterName, holder.getCounterCache(configuration),
            configuration);
      counter.init();
      return counter;
   }

   @Override
   public StrongCounter getStrongCounter(String name) {
      Object counter = counters.computeIfAbsent(name, this::createCounter);
      return validateCounter(StrongCounter.class, counter);
   }

   @Override
   public WeakCounter getWeakCounter(String name) {
      Object counter = counters.computeIfAbsent(name, this::createCounter);
      return validateCounter(WeakCounter.class, counter);
   }

   @Override
   public boolean defineCounter(String name, CounterConfiguration configuration) {
      validateConfiguration(configuration);
      CacheHolder holder = extractCacheHolder(future);
      //all the defined counters' configuration are persisted (if enabled)
      return holder != null && holder.addConfiguration(name, configuration);
   }

   @Override
   public boolean isDefined(String name) {
      CacheHolder holder = extractCacheHolder(future);
      return holder != null && holder.getConfiguration(name) != null;
   }

   @Override
   public CounterConfiguration getConfiguration(String counterName) {
      CacheHolder holder = extractCacheHolder(future);
      return holder == null ? null : holder.getConfiguration(counterName);
   }

   private Object createCounter(String counterName) {
      CacheHolder holder = extractCacheHolder(future);
      if (holder == null) {
         throw log.unableToFetchCaches();
      }
      CounterConfiguration configuration = holder.getConfiguration(counterName);
      if (configuration == null) {
         throw log.undefinedCounter(counterName);
      }
      switch (configuration.type()) {
         case WEAK:
            return createWeakCounter(counterName, configuration, holder);
         case BOUNDED_STRONG:
            return createBoundedStrongCounter(counterName, configuration, holder);
         case UNBOUNDED_STRONG:
            return createUnboundedStrongCounter(counterName, configuration, holder);
         default:
            throw new IllegalStateException("[should never happen] unknown counter type: " + configuration.type());
      }
   }

   private void validateConfiguration(CounterConfiguration configuration) {
      if (!allowPersistence && configuration.storage() == Storage.PERSISTENT) {
         throw log.invalidPersistentStorageMode();
      }
      switch (configuration.type()) {
         case BOUNDED_STRONG:
            if (Utils
                  .isValid(configuration.lowerBound(), configuration.initialValue(), configuration.upperBound())) {
               throw log.invalidInitialValueForBoundedCounter(configuration.lowerBound(), configuration.upperBound(),
                     configuration.initialValue());
            }
            break;
         case WEAK:
            if (configuration.concurrencyLevel() < 1) {
               throw log.invalidConcurrencyLevel(configuration.concurrencyLevel());
            }
            break;
      }
   }
}
