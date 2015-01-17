package org.infinispan.jcache;

import javax.cache.Cache;
import javax.cache.configuration.CompleteConfiguration;
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
      return cache.getConfiguration(CompleteConfiguration.class).isManagementEnabled();
   }

   @Override
   public String getKeyType() {
      return cache.getConfiguration(CompleteConfiguration.class).getKeyType().getName();
   }

   @Override
   public String getValueType() {
      return cache.getConfiguration(CompleteConfiguration.class).getValueType().getName();
   }

   @Override
   public boolean isReadThrough() {
      return cache.getConfiguration(CompleteConfiguration.class).isReadThrough();
   }

   @Override
   public boolean isStatisticsEnabled() {
      return cache.getConfiguration(CompleteConfiguration.class).isStatisticsEnabled();
   }

   @Override
   public boolean isStoreByValue() {
      return cache.getConfiguration(CompleteConfiguration.class).isStoreByValue();
   }

   @Override
   public boolean isWriteThrough() {
      return cache.getConfiguration(CompleteConfiguration.class).isWriteThrough();
   }

}
