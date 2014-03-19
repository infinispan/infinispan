package org.infinispan.eviction.impl;

import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.eviction.ActivationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Concrete implementation of activation logic manager.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@MBean(objectName = "Activation",
      description = "Component that handles activating entries that have been passivated to a CacheStore by loading them into memory.")
public class ActivationManagerImpl implements ActivationManager {

   private static final Log log = LogFactory.getLog(ActivationManagerImpl.class);

   private final AtomicLong activations = new AtomicLong(0);
   private PersistenceManager persistenceManager;
   private Configuration cfg;

   @ManagedAttribute(description = "Enables or disables the gathering of statistics by this component", displayName = "Statistics enabled", writable = true)
   private boolean statisticsEnabled = false;

   @Inject
   public void inject(PersistenceManager clm, Configuration cfg) {
      this.persistenceManager = clm;
      this.cfg = cfg;
   }

   @Start(priority = 11) // After the cache loader manager, before the passivation manager
   public void start() {
      statisticsEnabled = cfg.jmxStatistics().enabled();
   }

   @Override
   public void activate(Object key) {
      try {
         if (persistenceManager.activate(key) && statisticsEnabled) {
            activations.incrementAndGet();
         }
      } catch (CacheException e) {
         log.unableToRemoveEntryAfterActivation(key, e);
      }
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

