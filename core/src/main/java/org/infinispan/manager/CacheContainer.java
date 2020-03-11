package org.infinispan.manager;

import org.infinispan.Cache;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.commons.api.CacheContainerAdmin;

public interface CacheContainer extends BasicCacheContainer {
   @Override
   <K, V> Cache<K, V> getCache();

   @Override
   <K, V> Cache<K, V> getCache(String cacheName);

   /**
    * Provides access to administrative methods which affect the underlying cache container, such as cache creation and
    * removal. If the underlying container is clustered or remote, the operations will affect all nodes.
    */
   default CacheContainerAdmin<?, ?> administration() {
      throw new UnsupportedOperationException();
   }

}
