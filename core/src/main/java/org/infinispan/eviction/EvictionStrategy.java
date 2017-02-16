package org.infinispan.eviction;

/**
 * Supported eviction strategies
 *
 * @author Manik Surtani
 * @since 4.0
 */
public enum EvictionStrategy {
   NONE,
   UNORDERED,
   /*
    *
    * FIFO strategy is deprecated, LRU will be used instead
    */
   @Deprecated
   FIFO,
   LRU,
   LIRS,
   MANUAL;

   public boolean isEnabled() {
      return this != NONE && this != MANUAL;
   }
}
