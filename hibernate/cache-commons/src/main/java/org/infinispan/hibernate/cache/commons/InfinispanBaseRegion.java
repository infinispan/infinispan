package org.infinispan.hibernate.cache.commons;

import org.infinispan.AdvancedCache;

/**
 * Any region using {@link AdvancedCache} for the underlying storage.
 */
public interface InfinispanBaseRegion extends TimeSource {
   AdvancedCache getCache();

   String getName();

   default void invalidateRegion() {
      beginInvalidation();
      endInvalidation();
   }

   void beginInvalidation();

   void endInvalidation();

   long getLastRegionInvalidation();

   boolean checkValid();
}
