package org.infinispan.counter.impl.manager;

import static org.infinispan.counter.impl.Util.awaitCounterOperation;
import static org.infinispan.counter.util.Utils.validateStrongCounterBounds;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.PropertyFormatter;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.counter.impl.listener.CounterManagerNotificationManager;
import org.infinispan.counter.impl.strong.AbstractStrongCounter;
import org.infinispan.counter.impl.strong.BoundedStrongCounter;
import org.infinispan.counter.impl.strong.UnboundedStrongCounter;
import org.infinispan.counter.impl.weak.WeakCounterImpl;
import org.infinispan.counter.logging.Log;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * A {@link CounterManager} implementation for embedded cache manager.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@Scope(Scopes.GLOBAL)
@MBean(objectName = EmbeddedCounterManager.OBJECT_NAME, description = "Component to manage counters")
public class EmbeddedCounterManager implements CounterManager {
   public static final String OBJECT_NAME = "CounterManager";
   private static final long WAIT_CACHES_TIMEOUT = TimeUnit.SECONDS.toNanos(15);
   private static final Log log = LogFactory.getLog(EmbeddedCounterManager.class, Log.class);

   private final Map<String, Object> counters;
   private final CompletableFuture<CacheHolder> future;
   private final boolean allowPersistence;
   private final CounterManagerNotificationManager notificationManager;

   @Inject @ComponentName(KnownComponentNames.ASYNC_OPERATIONS_EXECUTOR)
   private Executor asyncExecutor;

   public EmbeddedCounterManager(CompletableFuture<CacheHolder> future, boolean allowPersistence) {
      this.allowPersistence = allowPersistence;
      this.counters = new ConcurrentHashMap<>();
      this.future = future;
      this.notificationManager = new CounterManagerNotificationManager();
   }

   @Start
   public void start() {
      notificationManager.useExecutor(asyncExecutor);
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
         CacheHolder holder, CounterManagerNotificationManager notificationManager) {
      WeakCounterImpl counter = new WeakCounterImpl(counterName, holder.getCounterCache(configuration), configuration,
            notificationManager);
      counter.init();
      return counter;
   }

   private static StrongCounter createBoundedStrongCounter(String counterName, CounterConfiguration configuration,
         CacheHolder holder, CounterManagerNotificationManager notificationManager) {
      BoundedStrongCounter counter = new BoundedStrongCounter(counterName, holder.getCounterCache(configuration),
            configuration, notificationManager);
      counter.init();
      return counter;
   }

   private static StrongCounter createUnboundedStrongCounter(String counterName, CounterConfiguration configuration,
         CacheHolder holder, CounterManagerNotificationManager notificationManager) {
      UnboundedStrongCounter counter = new UnboundedStrongCounter(counterName, holder.getCounterCache(configuration),
            configuration, notificationManager);
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
      return awaitCounterOperation(defineCounterAsync(name, configuration));
   }

   @Override
   public boolean isDefined(String name) {
      return awaitCounterOperation(isDefinedAsync(name));
   }

   @Override
   public CounterConfiguration getConfiguration(String counterName) {
      return awaitCounterOperation(getConfigurationAsync(counterName));
   }

   @ManagedOperation(
         description = "Removes the counter's value from the cluster. The counter will be re-created when access next time.",
         displayName = "Remove Counter",
         name = "remove"
   )
   @Override
   public void remove(String counterName) {
      CacheHolder holder = extractCacheHolder(future);
      if (holder == null) {
         throw log.unableToFetchCaches();
      }
      CounterConfiguration configuration = awaitCounterOperation(holder.getConfigurationAsync(counterName));
      if (configuration == null) {
         //counter not defined (cluster-wide). do nothing :)
         return;
      }
      counters.compute(counterName, (name, counter) -> {
         removeCounter(name, counter, configuration, holder);
         return null;
      });
   }

   @ManagedOperation(
         description = "Returns a collection of defined counter's name.",
         displayName = "Get Defined Counters",
         name = "counters")
   @Override
   public Collection<String> getCounterNames() {
      CacheHolder holder = extractCacheHolder(future);
      if (holder == null) {
         return Collections.emptyList();
      }
      return holder.getCounterNames();
   }

   @ManagedOperation(
         description = "Returns the current counter's value",
         displayName = "Get Counter' Value",
         name = "value"
   )
   public long getValue(String counterName) {
      CounterConfiguration configuration = getConfiguration(counterName);
      if (configuration == null) {
         throw log.undefinedCounter(counterName);
      }
      if (configuration.type() == CounterType.WEAK) {
         return getWeakCounter(counterName).getValue();
      } else {
         return awaitCounterOperation(getStrongCounter(counterName).getValue());
      }
   }

   @ManagedOperation(
         description = "Resets the counter's value",
         displayName = "Reset Counter",
         name = "reset"
   )
   public void reset(String counterName) {
      CounterConfiguration configuration = getConfiguration(counterName);
      if (configuration == null) {
         throw log.undefinedCounter(counterName);
      }
      if (configuration.type() == CounterType.WEAK) {
         awaitCounterOperation(getWeakCounter(counterName).reset());
      } else {
         awaitCounterOperation(getStrongCounter(counterName).reset());
      }
   }

   @ManagedOperation(
         description = "Returns the counter's configuration",
         displayName = "Counter Configuration",
         name = "configuration"
   )
   public Properties getCounterConfiguration(String counterName) {
      CounterConfiguration configuration = getConfiguration(counterName);
      if (configuration == null) {
         throw log.undefinedCounter(counterName);
      }
      return PropertyFormatter.getInstance().format(configuration);
   }

   public CompletableFuture<Boolean> defineCounterAsync(String name, CounterConfiguration configuration) {
      validateConfiguration(configuration);
      CacheHolder holder = extractCacheHolder(future);
      return holder == null ?
             CompletableFuture.completedFuture(false) :
             holder.addConfigurationAsync(name, configuration);
   }

   public CompletableFuture<CounterConfiguration> getConfigurationAsync(String name) {
      CacheHolder holder = extractCacheHolder(future);
      return holder == null ?
             CompletableFutures.completedNull() :
             holder.getConfigurationAsync(name);
   }

   public CompletableFuture<Boolean> isDefinedAsync(String name) {
      return getConfigurationAsync(name).thenApply(Objects::nonNull);
   }

   private void removeCounter(String name, Object counter, CounterConfiguration configuration,
         CacheHolder holder) {
      if (configuration.type() == CounterType.WEAK) {
         if (counter ==  null) {
            //no instance stored locally. Remove from cache only.
            WeakCounterImpl.removeWeakCounter(holder.getCounterCache(configuration), configuration, name);
         } else {
            ((WeakCounterImpl) counter).destroyAndRemove();
         }
      } else {
         if (counter == null) {
            //no instance stored locally. Remove from cache only.
            AbstractStrongCounter.removeStrongCounter(holder.getCounterCache(configuration), name);
         } else {
            ((AbstractStrongCounter) counter).destroyAndRemove();
         }
      }
   }

   private Object createCounter(String counterName) {
      CacheHolder holder = extractCacheHolder(future);
      if (holder == null) {
         throw log.unableToFetchCaches();
      }
      CounterConfiguration configuration = awaitCounterOperation(holder.getConfigurationAsync(counterName));
      if (configuration == null) {
         throw log.undefinedCounter(counterName);
      }
      holder.registerNotificationManager(notificationManager);
      switch (configuration.type()) {
         case WEAK:
            return createWeakCounter(counterName, configuration, holder, notificationManager);
         case BOUNDED_STRONG:
            return createBoundedStrongCounter(counterName, configuration, holder, notificationManager);
         case UNBOUNDED_STRONG:
            return createUnboundedStrongCounter(counterName, configuration, holder, notificationManager);
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
            validateStrongCounterBounds(configuration.lowerBound(), configuration.initialValue(),
                  configuration.upperBound());
            break;
         case WEAK:
            if (configuration.concurrencyLevel() < 1) {
               throw log.invalidConcurrencyLevel(configuration.concurrencyLevel());
            }
            break;
      }
   }
}
