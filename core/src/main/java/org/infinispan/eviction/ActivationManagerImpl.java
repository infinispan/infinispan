package org.infinispan.eviction;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
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
   private static final boolean trace = log.isTraceEnabled();

   private final AtomicLong activations = new AtomicLong(0);
   private CacheLoaderManager clm;
   private CacheStore store;
   private Configuration cfg;
   private boolean enabled;

   @ManagedAttribute(description = "Enables or disables the gathering of statistics by this component", displayName = "Statistics enabled", writable = true)
   private boolean statisticsEnabled = false;

   @Inject
   public void inject(CacheLoaderManager clm, Configuration cfg) {
      this.clm = clm;
      this.cfg = cfg;
   }

   @Start(priority = 10) // Just before the passivation manager
   public void start() {
      enabled = clm.isUsingPassivation() && !clm.isShared();
      if (enabled) {
         store = clm.getCacheStore();
         if (store == null)
            throw new CacheConfigurationException(
                  "Passivation can only be used with a CacheLoader that implements CacheStore!");

         statisticsEnabled = cfg.jmxStatistics().enabled();
      }
   }

   @Override
   public void activate(Object key) {
      if (enabled) {
         try {
            if (trace)
               log.tracef("Try to activate key=%s removing it from the store", key);

            if (store.remove(key) && statisticsEnabled) {
               activations.incrementAndGet();
            }
         } catch (CacheLoaderException e) {
            log.unableToRemoveEntryAfterActivation(key, e);
         }
      } else {
         if (trace)
            log.trace("Don't remove entry from shared cache store after activation.");
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

