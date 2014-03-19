package org.infinispan.manager;

import org.infinispan.Cache;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.api.BasicCacheContainer;

public interface CacheContainer extends BasicCacheContainer {
   /**
    * This method overrides the underlying {@link CacheContainer#getCache()},
    * to return a {@link Cache} instead of a {@link BasicCache}
    */
   @Override
   <K, V> Cache<K, V> getCache();

   /**
    * This method overrides the underlying {@link CacheContainer#getCache(String)},
    * to return a {@link Cache} instead of a {@link BasicCache}
    */
   @Override
   <K, V> Cache<K, V> getCache(String cacheName);

}
