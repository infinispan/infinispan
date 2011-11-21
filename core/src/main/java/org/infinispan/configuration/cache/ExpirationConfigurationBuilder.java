package org.infinispan.configuration.cache;

/**
 * Controls the default expiration settings for entries in the cache.
 */
public class ExpirationConfigurationBuilder extends AbstractConfigurationChildBuilder<ExpirationConfiguration> {

   private long lifespan;
   private long maxIdle;
   private boolean reaperEnabled;
   private long wakeUpInterval;
   
   ExpirationConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * Maximum lifespan of a cache entry, after which the entry is expired cluster-wide, in
    * milliseconds. -1 means the entries never expire.
    * 
    * Note that this can be overridden on a per-entry basis by using the Cache API.
    */
   public ExpirationConfigurationBuilder lifespan(long l) {
      this.lifespan = l;
      return this;
   }

   /**
    * Maximum idle time a cache entry will be maintained in the cache, in milliseconds. If the idle
    * time is exceeded, the entry will be expired cluster-wide. -1 means the entries never expire.
    * 
    * Note that this can be overridden on a per-entry basis by using the Cache API.
    */
   public ExpirationConfigurationBuilder maxIdle(long l) {
      this.maxIdle = l;
      return this;
   }

   /**
    * Enable the background reaper to test entries for expiration.
    * Regardless of whether a reaper is used, entries are tested for expiration lazily when they are
    * touched.
    */
   public ExpirationConfigurationBuilder enableReaper() {
      this.reaperEnabled = true;
      return this;
   }
   
   /**
    * Disable the background reaper to test entries for expiration. to test entries for expiration.
    * Regardless of whether a reaper is used, entries are tested for expiration lazily when they are
    * touched.
    */
   public ExpirationConfigurationBuilder disableReaper() {
      this.reaperEnabled = false;
      return this;
   }

   /**
    * Interval (in milliseconds) between subsequent runs to purge expired entries from memory and
    * any cache stores. If you wish to disable the periodic eviction process altogether, set
    * wakeupInterval to -1.
    */
   public ExpirationConfigurationBuilder wakeUpInterval(long l) {
      this.wakeUpInterval = l;
      return this;
   }

   @Override
   void validate() {
      // TODO Auto-generated method stub
      
   }

   @Override
   ExpirationConfiguration create() {
      return new ExpirationConfiguration(lifespan, maxIdle, reaperEnabled, wakeUpInterval);
   }

}
