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
   FIFO,
   LRU;
   
   public boolean isEnabled() {
      return this != NONE;
   }
}
