package org.infinispan.eviction.impl;

import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.BOTH;
import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.PRIVATE;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.eviction.ActivationManager;
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

   private final AtomicLong activations = new AtomicLong(0);

   @Inject PersistenceManager persistenceManager;
   @Inject Configuration cfg;
   @Inject DistributionManager distributionManager;
   @Inject KeyPartitioner keyPartitioner;

   private boolean passivation;

   @ManagedAttribute(description = "Enables or disables the gathering of statistics by this component", displayName = "Statistics enabled", writable = true)
   private boolean statisticsEnabled = false;

   @Start(priority = 11) // After the cache loader manager, before the passivation manager
   public void start() {
      statisticsEnabled = cfg.jmxStatistics().enabled();
      passivation = cfg.persistence().usingStores() && cfg.persistence().passivation();
   }

   @Override
   public void onUpdate(Object key, boolean newEntry) {
      if (!passivation || !newEntry) {
         //we don't have passivation or the entry already exists in container.
         return;
      }
      try {
         if (persistenceManager.deleteFromAllStoresSync(key, keyPartitioner.getSegment(key), PRIVATE)
               && statisticsEnabled) {
            activations.incrementAndGet();
         }
      } catch (CacheException e) {
         log.unableToRemoveEntryAfterActivation(key, e);
      }
   }

   @Override
   public CompletionStage<Void> onUpdateAsync(Object key, boolean newEntry) {
      if (!passivation || !newEntry) {
         //we don't have passivation or the entry already exists in container.
         return CompletableFutures.completedNull();
      }
      CompletionStage<Boolean> stage = persistenceManager.deleteFromAllStores(key, keyPartitioner.getSegment(key), PRIVATE);
      return stage.handle((removed, throwable) -> {
         if (throwable != null) {
            log.unableToRemoveEntryAfterActivation(key, throwable);
         } else if (statisticsEnabled && removed != null && removed) {
            activations.incrementAndGet();
         }
         return null;
      });
   }

   @Override
   public void onRemove(Object key, boolean newEntry) {
      if (!passivation) {
         return;
      }
      //if we are the primary owner, we need to remove from the shared store,
      final boolean primaryOwner = distributionManager != null && distributionManager.getCacheTopology().getDistribution(key).isPrimary();
      try {
         if (newEntry) {
            //the entry does not exists in data container. We need to remove from private and shared stores.
            //if we are the primary owner
            PersistenceManager.AccessMode mode = primaryOwner ? BOTH : PRIVATE;
            if (persistenceManager.deleteFromAllStoresSync(key, keyPartitioner.getSegment(key), mode) && statisticsEnabled) {
               activations.incrementAndGet();
            }
         } else {
            //the entry already exists in data container. It may be put during the load by the CacheLoaderInterceptor
            //so it was already activate in the private stores.
            if (primaryOwner && persistenceManager.deleteFromAllStoresSync(key, keyPartitioner.getSegment(key), BOTH) &&
                  statisticsEnabled) {
               activations.incrementAndGet();
            }
         }

      } catch (CacheException e) {
         log.unableToRemoveEntryAfterActivation(key, e);
      }
   }

   @Override
   public CompletionStage<Void> onRemoveAsync(Object key, boolean newEntry) {
      if (!passivation) {
         return CompletableFutures.completedNull();
      }
      //if we are the primary owner, we need to remove from the shared store,
      final boolean primaryOwner = distributionManager != null && distributionManager.getCacheTopology().getDistribution(key).isPrimary();
      CompletionStage<Boolean> stage = null;
      if (newEntry) {
         //the entry does not exists in data container. We need to remove from private and shared stores.
         //if we are the primary owner
         stage = persistenceManager.deleteFromAllStores(key, keyPartitioner.getSegment(key),
               primaryOwner ? BOTH : PRIVATE);
      } else {
         //the entry already exists in data container. It may be put during the load by the CacheLoaderInterceptor
         //so it was already activate in the private stores.
         if (primaryOwner) {
            stage = persistenceManager.deleteFromAllStores(key, keyPartitioner.getSegment(key), BOTH);
         }
      }

      if (stage != null) {
         return stage.handle((removed, throwable) -> {
            if (throwable != null) {
               log.unableToRemoveEntryAfterActivation(key, throwable);
            } else if (statisticsEnabled && removed != null && removed) {
               activations.incrementAndGet();
            }
            return null;
         });
      }
      return CompletableFutures.completedNull();
   }

   @Override
   public long getActivationCount() {
      return activations.get();
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
      activations.set(0);
   }
}
