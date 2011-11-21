package org.infinispan.configuration.cache;

/**
 * Configures the L1 cache behavior in 'distributed' caches instances. In any other cache modes,
 * this element is ignored.
 */

public class L1Configuration {

   private final boolean enabled;
   private final int invalidationThreshold;
   private final long lifespan;
   private final boolean onRehash;

   L1Configuration(boolean enabled, int invalidationThreshold, long lifespan, boolean onRehash) {
      this.enabled = enabled;
      this.invalidationThreshold = invalidationThreshold;
      this.lifespan = lifespan;
      this.onRehash = onRehash;
   }

   public boolean isEnabled() {
      return enabled;
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
    * If the threshold is set to -1, then unicasts will always be used. If the threshold is set to 0, then multicast 
    * will be always be used.
    * </p> 
    */
   public int getInvalidationThreshold() {
      return invalidationThreshold;
   }

   /**
    * Maximum lifespan of an entry placed in the L1 cache.
    */
   public long getLifespan() {
      return lifespan;
   }

   /**
    * If true, entries removed due to a rehash will be moved to L1 rather than being removed
    * altogether.
    */
   public boolean isOnRehash() {
      return onRehash;
   }

}
