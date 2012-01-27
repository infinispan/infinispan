package org.infinispan.configuration.cache;

import org.infinispan.config.ConfigurationException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.TimeUnit;

/**
 * Configures the L1 cache behavior in 'distributed' caches instances. In any other cache modes,
 * this element is ignored.
 */

public class L1ConfigurationBuilder extends AbstractClusteringConfigurationChildBuilder<L1Configuration> {

   private static final Log log = LogFactory.getLog(L1ConfigurationBuilder.class);
   
   private boolean enabled = true;
   private int invalidationThreshold = 0;
   private long lifespan = TimeUnit.MINUTES.toMillis(10);
   private Boolean onRehash = null;
   boolean activated = false;

   L1ConfigurationBuilder(ClusteringConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * <p>
    * Determines whether a multicast or a web of unicasts are used when performing L1 invalidations.
    * </p>
    * 
    * <p>
    * By default multicast will be used.
    * </p>
    * 
    * <p>
    * If the threshold is set to -1, then unicasts will always be used. If the threshold is set to
    * 0, then multicast will be always be used.
    * </p>
    * 
    * @param threshold the threshold over which to use a multicast
    * 
    */
   public L1ConfigurationBuilder invalidationThreshold(int invalidationThreshold) {
      this.invalidationThreshold = invalidationThreshold;
      activated = true;
      return this;
   }

   /**
    * Maximum lifespan of an entry placed in the L1 cache.
    */
   public L1ConfigurationBuilder lifespan(long livespan) {
      this.lifespan = livespan;
      activated = true;
      return this;
   }

   /**
    * Entries removed due to a rehash will be moved to L1 rather than being removed altogether.
    */
   public L1ConfigurationBuilder enableOnRehash() {
      this.onRehash = true;
      activated = true;
      return this;
   }
   
   /**
    * Entries removed due to a rehash will be moved to L1 rather than being removed altogether.
    */
   public L1ConfigurationBuilder onRehash(boolean enabled) {
      this.onRehash = enabled;
      activated = true;
      return this;
   }

   /**
    * Entries removed due to a rehash will be removed altogether rather than bring moved to L1.
    */
   public L1ConfigurationBuilder disableOnRehash() {
      this.onRehash = false;
      activated = true;
      return this;
   }
   
   public L1ConfigurationBuilder enable() {
      this.enabled = true;
      activated = true;
      return this;
   }
   
   public L1ConfigurationBuilder disable() {
      this.enabled = false;
      activated = true;
      return this;
   }
   
   public L1ConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      activated = true;
      return this;
   }

   @Override
   void validate() {
      if (enabled) {
         if (!clustering().cacheMode().isDistributed() && activated)
            throw new ConfigurationException("Enabling the L1 cache is only supported when using DISTRIBUTED as a cache mode.  Your cache mode is set to " + clustering().cacheMode().friendlyCacheModeString());

         if (lifespan < 1)
            throw new ConfigurationException("Using a L1 lifespan of 0 or a negative value is meaningless");
      } else {
         // If L1 is disabled, L1ForRehash should also be disabled
         if (onRehash != null && onRehash)
            throw new ConfigurationException("Can only move entries to L1 on rehash when L1 is enabled");
      }
   }

   @Override
   L1Configuration create() {
      
      if (enabled && onRehash == null) {
         log.debug("L1 is enabled and L1OnRehash was not defined, enabling it");
         onRehash = true;
      } else
         onRehash = false;
      
      return new L1Configuration(enabled, invalidationThreshold, lifespan, onRehash, activated);
   }
   
   @Override
   public L1ConfigurationBuilder read(L1Configuration template) {
      enabled = template.enabled();
      invalidationThreshold = template.invalidationThreshold();
      lifespan = template.lifespan();
      onRehash = template.onRehash();
      activated = template.activated;
      return this;
   }

}
