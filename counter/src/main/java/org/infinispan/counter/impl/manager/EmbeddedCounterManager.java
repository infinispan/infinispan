package org.infinispan.counter.impl.manager;

import static org.infinispan.counter.impl.Util.awaitCounterOperation;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.context.Flag;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.PropertyFormatter;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.counter.exception.CounterException;
import org.infinispan.counter.impl.CounterModuleLifecycle;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.counter.impl.entries.CounterValue;
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
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.manager.EmbeddedCacheManager;

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
   private static final Log log = LogFactory.getLog(EmbeddedCounterManager.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private final Map<String, Object> counters;
   private final CounterManagerNotificationManager notificationManager;
   private final EmbeddedCacheManager cacheManager;
   private final CounterConfigurationManager configurationManager;
   private volatile AdvancedCache<CounterKey, CounterValue> counterCache;
   private volatile boolean started = false;

   @Inject @ComponentName(KnownComponentNames.ASYNC_OPERATIONS_EXECUTOR)
   private Executor asyncExecutor;

   public EmbeddedCounterManager(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
      this.counters = new ConcurrentHashMap<>();
      this.notificationManager = new CounterManagerNotificationManager();
      CounterConfigurationStorage storage = isGlobalStateEnabled(cacheManager) ?
            new PersistedCounterConfigurationStorage() :
            new VolatileCounterConfigurationStorage();
      storage.initialize(cacheManager);
      this.configurationManager = new CounterConfigurationManager(cacheManager, storage);
   }

   private static boolean isGlobalStateEnabled(EmbeddedCacheManager cacheManager) {
      return cacheManager.getGlobalComponentRegistry().getGlobalConfiguration().globalState().enabled();
   }

   @Start
   public void start() {
      if (trace) {
         log.trace("Starting EmbeddedCounterManager");
      }
      notificationManager.useExecutor(asyncExecutor);

      configurationManager.start();
      started = true;
   }

   @Stop(priority = 9) //lower than default priority to avoid creating the counters cache.
   public void stop() {
      if (trace) {
         log.trace("Stopping EmbeddedCounterManager");
      }
      started = false;
      counterCache = null;
      configurationManager.stop();
      notificationManager.stop();
   }

   private static <T> T validateCounter(Class<T> tClass, Object retVal) {
      Class<?> rClass = retVal.getClass();
      if (tClass.isAssignableFrom(rClass)) {
         return tClass.cast(retVal);
      }
      throw log.invalidCounterType(tClass.getSimpleName(), rClass.getSimpleName());
   }

   @ManagedOperation(
         description = "Removes the counter's value from the cluster. The counter will be re-created when access next time.",
         displayName = "Remove Counter",
         name = "remove"
   )
   @Override
   public void remove(String counterName) {
      CounterConfiguration configuration = getConfiguration(counterName);
      if (configuration == null) {
         //counter not defined (cluster-wide). do nothing :)
         return;
      }
      counters.compute(counterName, (name, counter) -> {
         removeCounter(name, counter, configuration);
         return null;
      });
   }

   @Override
   public StrongCounter getStrongCounter(String name) {
      checkStarted();
      Object counter = counters.computeIfAbsent(name, this::createCounter);
      return validateCounter(StrongCounter.class, counter);
   }

   @Override
   public WeakCounter getWeakCounter(String name) {
      checkStarted();
      Object counter = counters.computeIfAbsent(name, this::createCounter);
      return validateCounter(WeakCounter.class, counter);
   }

   @ManagedOperation(
         description = "Returns a collection of defined counter's name.",
         displayName = "Get Defined Counters",
         name = "counters")
   @Override
   public Collection<String> getCounterNames() {
      return configurationManager.getCounterNames();
   }

   public CompletableFuture<Boolean> defineCounterAsync(String name, CounterConfiguration configuration) {
      return configurationManager.defineConfiguration(name, configuration);
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

   public CompletableFuture<CounterConfiguration> getConfigurationAsync(String name) {
      return configurationManager.getConfiguration(name);
   }

   private StrongCounter createBoundedStrongCounter(String counterName, CounterConfiguration configuration) {
      BoundedStrongCounter counter = new BoundedStrongCounter(counterName, cache(configuration), configuration,
            notificationManager);
      counter.init();
      return counter;
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

   private StrongCounter createUnboundedStrongCounter(String counterName, CounterConfiguration configuration) {
      UnboundedStrongCounter counter = new UnboundedStrongCounter(counterName, cache(configuration), configuration,
            notificationManager);
      counter.init();
      return counter;
   }

   private WeakCounter createWeakCounter(String counterName, CounterConfiguration configuration) {
      WeakCounterImpl counter = new WeakCounterImpl(counterName, cache(configuration), configuration,
            notificationManager);
      counter.init();
      return counter;
   }

   public CompletableFuture<Boolean> isDefinedAsync(String name) {
      return getConfigurationAsync(name).thenApply(Objects::nonNull);
   }

   private synchronized void assertCounterCacheCreated() {
      //checks and waits until cache is started
      if (started && counterCache == null) {
         counterCache = cacheManager.<CounterKey, CounterValue>getCache(CounterModuleLifecycle.COUNTER_CACHE_NAME)
               .getAdvancedCache();
      }
   }

   private <K extends CounterKey> AdvancedCache<K, CounterValue> cache() {
      assertCounterCacheCreated();
      //noinspection unchecked
      return (AdvancedCache<K, CounterValue>) counterCache;
   }

   private <K extends CounterKey> AdvancedCache<K, CounterValue> cache(CounterConfiguration configuration) {
      return configuration.storage() == Storage.VOLATILE ?
            this.<K>cache().withFlags(Flag.SKIP_CACHE_LOAD, Flag.SKIP_CACHE_STORE) :
            cache();
   }

   private void removeCounter(String name, Object counter, CounterConfiguration configuration) {
      if (configuration.type() == CounterType.WEAK) {
         if (counter ==  null) {
            //no instance stored locally. Remove from cache only.
            WeakCounterImpl.removeWeakCounter(cache(), configuration, name);
         } else {
            ((WeakCounterImpl) counter).destroyAndRemove();
         }
      } else {
         if (counter == null) {
            //no instance stored locally. Remove from cache only.
            AbstractStrongCounter.removeStrongCounter(cache(), name);
         } else {
            ((AbstractStrongCounter) counter).destroyAndRemove();
         }
      }
   }

   private void checkStarted() {
      if (!started) {
         throw log.managerNotStarted();
      }
   }

   private Object createCounter(String counterName) {
      CounterConfiguration configuration = getConfiguration(counterName);
      if (configuration == null) {
         throw log.undefinedCounter(counterName);
      }

      //puts the listeners in there (topology and for counter's event)
      //this method only registers only once
      //cache() ensures the cache is started!
      try {
         notificationManager.listenOn(cache());
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CounterException(e);
      }

      switch (configuration.type()) {
         case WEAK:
            return createWeakCounter(counterName, configuration);
         case BOUNDED_STRONG:
            return createBoundedStrongCounter(counterName, configuration);
         case UNBOUNDED_STRONG:
            return createUnboundedStrongCounter(counterName, configuration);
         default:
            throw new IllegalStateException("[should never happen] unknown counter type: " + configuration.type());
      }
   }
}
