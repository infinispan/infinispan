package org.infinispan.jcache.remote;

import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.MutableConfiguration;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;

public class ConfigurationAdapter<K, V> {

   private MutableConfiguration<K, V> c;

   private ConfigurationAdapter(MutableConfiguration<K, V> configuration) {
      this.c = configuration;
   }

   public MutableConfiguration<K, V> getConfiguration() {
      return c;
   }

   public org.infinispan.client.hotrod.configuration.Configuration build() {
      ConfigurationBuilder cb = new ConfigurationBuilder();

      //TODO:!
//      if (c.isStoreByValue())
//         cb.storeAsBinary().enable();
//
//      Factory<CacheLoader<K,V>> cacheLoaderFactory = c.getCacheLoaderFactory();
//      if (cacheLoaderFactory != null) {
//         // User-defined cache loader will be plugged once cache has started
//         cb.persistence().addStore(JStoreAdapterConfigurationBuilder.class);
//      }
//
//      Factory<CacheWriter<? super K, ? super V>> cacheWriterFactory = c.getCacheWriterFactory();
//      if (cacheWriterFactory != null) {
//         // User-defined cache writer will be plugged once cache has started
//         cb.persistence().addStore(JCacheWriterAdapterConfigurationBuilder.class);
//      }
//
//      if (c.isStatisticsEnabled())
//         cb.jmxStatistics().enable();

      return cb.build();
   }

   public static <K, V> ConfigurationAdapter<K, V> create(Configuration<K, V> c) {
      // A configuration copy as required by the spec
      if (c instanceof CompleteConfiguration) {
         return new ConfigurationAdapter<K, V>(
               new MutableConfiguration<K, V>((CompleteConfiguration<K, V>) c));
      } else {
         //support use of Basic Configuration
         MutableConfiguration<K, V> mutableConfiguration = new MutableConfiguration<K, V>();
         mutableConfiguration.setStoreByValue(c.isStoreByValue());
         mutableConfiguration.setTypes(c.getKeyType(), c.getValueType());
         return new ConfigurationAdapter<K, V>(
               new MutableConfiguration<K, V>(mutableConfiguration));
      }
   }

   public static <K, V> ConfigurationAdapter<K, V> create() {
      return new ConfigurationAdapter<K, V>(new MutableConfiguration<K, V>());
   }
}
