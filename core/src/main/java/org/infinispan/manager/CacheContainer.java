package org.infinispan.manager;

import org.infinispan.BasicCache;
import org.infinispan.Cache;

public interface CacheContainer extends BasicCacheContainer {
   /**
    * This method overrides the underlying {@link CacheContainer#getCache()}, 
    * to return a {@link Cache} instead of a {@link BasicCache} 
    */
   <K, V> Cache<K, V> getCache();
   
   /**
    * This method overrides the underlying {@link CacheContainer#getCache(String)}, 
    * to return a {@link Cache} instead of a {@link BasicCache} 
    */
   <K, V> Cache<K, V> getCache(String cacheName);

}
