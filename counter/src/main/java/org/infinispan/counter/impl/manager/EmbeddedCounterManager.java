package org.infinispan.counter.impl.manager;

import static org.infinispan.commons.util.concurrent.CompletableFutures.completedNull;
import static org.infinispan.commons.util.concurrent.CompletableFutures.toNullFunction;
import static org.infinispan.counter.impl.Util.awaitCounterOperation;
import static org.infinispan.counter.logging.Log.CONTAINER;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.PropertyFormatter;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.counter.impl.factory.StrongCounterFactory;
import org.infinispan.counter.impl.factory.WeakCounterFactory;
import org.infinispan.counter.impl.listener.CounterManagerNotificationManager;
import org.infinispan.counter.logging.Log;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.util.concurrent.CompletionStages;

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

   private final Map<String, CompletableFuture<InternalCounterAdmin>> counters;
   private volatile boolean stopped = true;

   @Inject CounterConfigurationManager configurationManager;
   @Inject CounterManagerNotificationManager notificationManager;
   @Inject StrongCounterFactory strongCounterFactory;
   @Inject WeakCounterFactory weakCounterFactory;

   public EmbeddedCounterManager() {
      counters = new ConcurrentHashMap<>(32);
   }

   @Start
   public void start() {
      if (log.isTraceEnabled()) {
         log.trace("Starting EmbeddedCounterManager");
      }
      stopped = false;
   }

   @Stop(priority = 9) //lower than default priority to avoid creating the counters cache.
   public void stop() {
      if (log.isTraceEnabled()) {
         log.trace("Stopping EmbeddedCounterManager");
      }
      stopped = true;
   }

   @ManagedOperation(
         description = "Removes the counter's value from the cluster. The counter will be re-created when access next time.",
         displayName = "Remove Counter",
         name = "remove"
   )
   @Override
   public void remove(String counterName) {
      awaitCounterOperation(removeAsync(counterName, true));
   }

   public CompletionStage<Void> removeAsync(String counterName, boolean keepConfig) {
      CompletionStage<Void> removeStage = getConfigurationAsync(counterName)
            .thenCompose(config -> {
               // check if the counter is defined
               if (config == null) {
                  return completedNull();
               }

               CompletionStage<InternalCounterAdmin> existingCounter = counters.remove(counterName);
               // if counter instance exists, invoke destroy()
               if (existingCounter != null) {
                  return existingCounter.thenCompose(InternalCounterAdmin::destroy);
               }

               if (config.type() == CounterType.WEAK) {
                  return weakCounterFactory.removeWeakCounter(counterName, config);
               } else {
                  return strongCounterFactory.removeStrongCounter(counterName);
               }
            });
      if (keepConfig) {
         return removeStage;
      }
      return removeStage.thenCompose(unused -> configurationManager.removeConfiguration(counterName))
            .thenApply(toNullFunction());
   }

   @Override
   public void undefineCounter(String counterName) {
      awaitCounterOperation(removeAsync(counterName, false));
   }

   @Override
   public StrongCounter getStrongCounter(String name) {
      return awaitCounterOperation(getStrongCounterAsync(name));
   }

   public CompletionStage<StrongCounter> getStrongCounterAsync(String counterName) {
      return getOrCreateAsync(counterName).thenApply(InternalCounterAdmin::asStrongCounter);
   }

   @Override
   public WeakCounter getWeakCounter(String name) {
      return awaitCounterOperation(getWeakCounterAsync(name));
   }

   public CompletionStage<WeakCounter> getWeakCounterAsync(String counterName) {
      return getOrCreateAsync(counterName).thenApply(InternalCounterAdmin::asWeakCounter);
   }

   public CompletionStage<InternalCounterAdmin> getOrCreateAsync(String counterName) {
      if (stopped) {
         return CompletableFuture.failedFuture(CONTAINER.counterManagerNotStarted());
      }
      CompletableFuture<InternalCounterAdmin> stage = counters.computeIfAbsent(counterName, this::createCounter);

      if (CompletionStages.isCompletedSuccessfully(stage)) {
         // avoid invoking "exceptionally()" every time
         return stage;
      }

      // remove if it fails
      stage.exceptionally(throwable -> {
         counters.remove(counterName, stage);
         return null;
      });

      return stage;
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

   @ManagedOperation(
         description = "Returns the current counter's value",
         displayName = "Get Counter' Value",
         name = "value"
   )
   public long getValue(String counterName) {
      return awaitCounterOperation(getOrCreateAsync(counterName).thenCompose(InternalCounterAdmin::value));
   }

   @ManagedOperation(
         description = "Resets the counter's value",
         displayName = "Reset Counter",
         name = "reset"
   )
   public void reset(String counterName) {
      awaitCounterOperation(getOrCreateAsync(counterName).thenCompose(InternalCounterAdmin::reset));
   }

   @ManagedOperation(
         description = "Returns the counter's configuration",
         displayName = "Counter Configuration",
         name = "configuration"
   )
   public Properties getCounterConfiguration(String counterName) {
      CounterConfiguration configuration = getConfiguration(counterName);
      if (configuration == null) {
         throw CONTAINER.undefinedCounter(counterName);
      }
      return PropertyFormatter.getInstance().format(configuration);
   }

   public CompletableFuture<Boolean> isDefinedAsync(String name) {
      return getConfigurationAsync(name).thenApply(Objects::nonNull);
   }

   private CompletableFuture<InternalCounterAdmin> createCounter(String counterName) {
      return getConfigurationAsync(counterName)
            .thenCompose(config -> {
               if (config == null) {
                  return CompletableFuture.failedFuture(CONTAINER.undefinedCounter(counterName));
               }
               switch (config.type()) {
                  case WEAK:
                     return weakCounterFactory.createWeakCounter(counterName, config);
                  case BOUNDED_STRONG:
                  case UNBOUNDED_STRONG:
                     return strongCounterFactory.createStrongCounter(counterName, config);
                  default:
                     return CompletableFuture.failedFuture(new IllegalStateException("[should never happen] unknown counter type: " + config.type()));
               }
            });
   }
}
