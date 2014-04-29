package org.infinispan.configuration.cache;

/**
 * Configures the L1 cache behavior in 'distributed' caches instances. In any other cache modes,
 * this element is ignored.
 */

public class L1Configuration {

   private final boolean enabled;
   private final int invalidationThreshold;
   private final long lifespan;
   private final long cleanupTaskFrequency;

   L1Configuration(boolean enabled, int invalidationThreshold, long lifespan, long cleanupTaskFrequency) {
      this.enabled = enabled;
      this.invalidationThreshold = invalidationThreshold;
      this.lifespan = lifespan;
      this.cleanupTaskFrequency = cleanupTaskFrequency;
   }

   public boolean enabled() {
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
   public int invalidationThreshold() {
      return invalidationThreshold;
   }

   /**
    * Determines how often a cleanup thread runs to clean up an internal log of requestors for a specific key
    */
   public long cleanupTaskFrequency() {
      return cleanupTaskFrequency;
   }


   /**
    * Maximum lifespan of an entry placed in the L1 cache. Default 10 minutes.
    */
   public long lifespan() {
      return lifespan;
   }

   @Override
   public String toString() {
      return "L1Configuration{" +
            "enabled=" + enabled +
            ", invalidationThreshold=" + invalidationThreshold +
            ", lifespan=" + lifespan +
            ", cleanupTaskFrequency=" + cleanupTaskFrequency +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      L1Configuration that = (L1Configuration) o;

      if (enabled != that.enabled) return false;
      if (invalidationThreshold != that.invalidationThreshold) return false;
      if (lifespan != that.lifespan) return false;
      if (cleanupTaskFrequency != that.cleanupTaskFrequency) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = (enabled ? 1 : 0);
      result = 31 * result + invalidationThreshold;
      result = 31 * result + (int) (lifespan ^ (lifespan >>> 32));
      result = 31 * result + (int) (cleanupTaskFrequency ^ (cleanupTaskFrequency >>> 32));
      return result;
   }

}
