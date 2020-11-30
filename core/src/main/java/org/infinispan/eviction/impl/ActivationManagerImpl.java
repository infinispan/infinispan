package org.infinispan.eviction.impl;

import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.PRIVATE;
import static org.infinispan.util.logging.Log.CONTAINER;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.LongAdder;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Concrete implementation of activation logic manager.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@MBean(objectName = "Activation",
      description = "Component that handles activating entries that have been passivated to a CacheStore by loading them into memory.")
@Scope(Scopes.NAMED_CACHE)
public class ActivationManagerImpl implements ActivationManager {

   private static final Log log = LogFactory.getLog(ActivationManagerImpl.class);

   private final LongAdder activations = new LongAdder();
   private final LongAdder pendingActivations = new LongAdder();

   @Inject PersistenceManager persistenceManager;
   @Inject Configuration cfg;
   @Inject DistributionManager distributionManager;
   @Inject KeyPartitioner keyPartitioner;

   private boolean passivation;

   private boolean statisticsEnabled = false;

   @Start(priority = 11) // After the cache loader manager, before the passivation manager
   public void start() {
      statisticsEnabled = cfg.statistics().enabled();
      passivation = cfg.persistence().usingStores() && cfg.persistence().passivation();
   }

   @Override
   public CompletionStage<Void> activateAsync(Object key, int segment) {
      if (!passivation) {
         return CompletableFutures.completedNull();
      }
      if (log.isTraceEnabled()) {
         log.tracef("Activating entry for key %s", key);
      }
      if (statisticsEnabled) {
         pendingActivations.increment();
      }
      CompletionStage<Boolean> stage = persistenceManager.deleteFromAllStores(key, segment, PRIVATE);
      return stage.handle((removed, throwable) -> {
         if (statisticsEnabled) {
            pendingActivations.decrement();
         }
         if (throwable != null) {
            CONTAINER.unableToRemoveEntryAfterActivation(key, throwable);
         } else if (statisticsEnabled && removed == Boolean.TRUE) {
            activations.increment();
         }
         return null;
      });
   }

   @ManagedAttribute(
         description = "Number of activation events",
         displayName = "Number of cache entries activated",
         measurementType = MeasurementType.TRENDSUP
   )
   @Override
   public long getActivationCount() {
      return activations.sum();
   }

   @ManagedAttribute(
         description = "Number of activation events",
         displayName = "Number of cache entries activated",
         measurementType = MeasurementType.TRENDSUP
   )
   public String getActivations() {
      if (!statisticsEnabled)
         return "N/A";

      return String.valueOf(getActivationCount());
   }

   @ManagedOperation(
         description = "Resets statistics gathered by this component",
         displayName = "Reset statistics"
   )
   public void resetStatistics() {
      activations.reset();
   }

   @ManagedAttribute(description = "Enables or disables the gathering of statistics by this component", displayName = "Statistics enabled", writable = true)
   public boolean getStatisticsEnabled() {
      return statisticsEnabled;
   }

   public void setStatisticsEnabled(boolean statisticsEnabled) {
      this.statisticsEnabled = statisticsEnabled;
   }

   @Override
   public long getPendingActivationCount() {
      return pendingActivations.sum();
   }
}
