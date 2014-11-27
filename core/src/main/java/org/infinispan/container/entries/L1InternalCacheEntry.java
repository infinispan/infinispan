package org.infinispan.container.entries;

/**
 * A {@link org.infinispan.container.entries.InternalCacheEntry} implementation to store a L1 entry.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public class L1InternalCacheEntry extends MortalCacheEntry {

   public L1InternalCacheEntry(Object key, Object value, long lifespan, long created) {
      super(key, value, lifespan, created);
   }

   @Override
   public boolean isL1Entry() {
      return true;
   }
}
