package org.infinispan.eviction;

/**
 * Supported eviction strategies
 *
 * @author Manik Surtani
 * @since 4.0
 */
public enum EvictionStrategy {
   // These will be removed later - they are changed to REMOVE
   @Deprecated
   UNORDERED(false, true),
   @Deprecated
   FIFO(false, true),
   @Deprecated
   LRU(false, true),
   @Deprecated
   LIRS(false, true),

   /**
    * Eviction Strategy where nothing is done by the cache and the user is probably not going to use eviction manually
    */
   NONE(false, false),
   /**
    * Strategy where the cache does nothing but the user is assumed to manually invoke evict method
    */
   MANUAL(false, false),
   /**
    * Strategy where the cache will remove entries to make room for new ones while staying under the configured size
    */
   REMOVE(false, true),
   /**
    * Strategy where the cache will block new entries from being written if they would exceed the configured size
    */
   EXCEPTION(true, false),
   ;

   private boolean exception;
   private boolean removal;

   EvictionStrategy(boolean exception, boolean removal) {
      this.exception = exception;
      this.removal = removal;
   }

   /**
    * Whether or not the cache will do something due to the strategy
    * @return
    */
   public boolean isEnabled() {
      return this != NONE && this != MANUAL;
   }

   /**
    * The cache will throw exceptions to prevent memory growth
    * @return
    */
   public boolean isExceptionBased() {
      return exception;
   }

   /**
    * The cache will remove other entries to make room to limit memory growth
    * @return
    */
   public boolean isRemovalBased() {
      return removal;
   }
}
