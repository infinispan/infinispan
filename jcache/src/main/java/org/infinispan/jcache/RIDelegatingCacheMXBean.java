package org.infinispan.jcache;

import javax.cache.Cache;
import javax.cache.management.CacheMXBean;

/**
 * Class to help implementers
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values*
 * @author Yannis Cosmadopoulos
 * @since 1.0
 */
public class RIDelegatingCacheMXBean<K, V> implements CacheMXBean {

   private final Cache<K, V> cache;

   /**
    * Constructor
    * @param cache the cache
    */
   public RIDelegatingCacheMXBean(Cache<K, V> cache) {
      this.cache = cache;
   }

   @Override
   public boolean isManagementEnabled() {
      return cache.getConfiguration().isManagementEnabled();
   }

   @Override
   public boolean isReadThrough() {
      return cache.getConfiguration().isReadThrough();
   }

   @Override
   public boolean isStatisticsEnabled() {
      return cache.getConfiguration().isStatisticsEnabled();
   }

   @Override
   public boolean isStoreByValue() {
      return cache.getConfiguration().isStoreByValue();
   }

   @Override
   public boolean isWriteThrough() {
      return cache.getConfiguration().isWriteThrough();
   }

}
