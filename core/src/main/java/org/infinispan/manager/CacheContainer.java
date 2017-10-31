package org.infinispan.manager;

import org.infinispan.Cache;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.commons.api.CacheContainerAdmin;

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

   /**
    * Provides access to administrative methods which affect the underlying cache container, such as cache creation and
    * removal. If the underlying container is clustered or remote, the operations will affect all nodes.
    * @return
    */
   default CacheContainerAdmin<?> administration() {
      throw new UnsupportedOperationException();
   }

}
