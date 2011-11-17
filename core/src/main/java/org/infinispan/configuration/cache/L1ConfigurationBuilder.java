package org.infinispan.configuration.cache;


/**
 * Configures the L1 cache behavior in 'distributed' caches instances. In any other cache modes,
 * this element is ignored.
 */

public class L1ConfigurationBuilder extends AbstractClusteringConfigurationChildBuilder<L1Configuration> {

   private boolean enabled = true;
   private int invalidationThreshold = 0;
   private long lifespan = 600000L;
   private boolean onRehash = true;
   

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
      return this;
   }

   /**
    * Maximum lifespan of an entry placed in the L1 cache.
    */
   public L1ConfigurationBuilder lifespan(long livespan) {
      this.lifespan = livespan;
      return this;
   }

   /**
    * Entries removed due to a rehash will be moved to L1 rather than being removed altogether.
    */
   public L1ConfigurationBuilder enableOnRehash() {
      this.onRehash = true;
      return this;
   }

   /**
    * Entries removed due to a rehash will be removed altogether rather than bring moved to L1.
    */
   public L1ConfigurationBuilder disableOnRehash() {
      this.onRehash = false;
      return this;
   }
   
   public L1ConfigurationBuilder enable() {
      this.enabled = true;
      return this;
   }
   
   public L1ConfigurationBuilder disable() {
      this.enabled = false;
      return this;
   }
   
   public L1ConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   @Override
   void validate() {
      // TODO Auto-generated method stub
      
   }

   @Override
   L1Configuration create() {
      return new L1Configuration(enabled, invalidationThreshold, lifespan, onRehash);
   }

}
