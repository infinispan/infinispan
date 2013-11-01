package org.infinispan.jcache;

import org.infinispan.configuration.cache.ConfigurationBuilder;

import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;

/**
 * ConfigurationAdapter takes {@link javax.cache.configuration.Configuration} and creates
 * equivalent instance of {@link org.infinispan.configuration.cache.Configuration}
 *
 * @author Vladimir Blagojevic
 * @author Galder Zamarreño
 * @since 5.3
 */
public class ConfigurationAdapter<K, V> {

   private javax.cache.configuration.Configuration<K, V> c;

   public ConfigurationAdapter(javax.cache.configuration.Configuration<K, V> configuration) {
      this.c = configuration;
   }

   public org.infinispan.configuration.cache.Configuration build() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      if (c.isStoreByValue())
         cb.storeAsBinary().enable();

      Factory<CacheLoader<K,V>> cacheLoaderFactory = c.getCacheLoaderFactory();
      if (cacheLoaderFactory != null) {
         // User-defined cache loader will be plugged once cache has started
         cb.persistence().addStore(JStoreAdapterConfigurationBuilder.class);
      }

      Factory<CacheWriter<? super K, ? super V>> cacheWriterFactory = c.getCacheWriterFactory();
      if (cacheWriterFactory != null) {
         // User-defined cache writer will be plugged once cache has started
         cb.persistence().addStore(JCacheWriterAdapterConfigurationBuilder.class);
      }

      if (c.isStatisticsEnabled())
         cb.jmxStatistics().enable();

      return cb.build();
   }
}
