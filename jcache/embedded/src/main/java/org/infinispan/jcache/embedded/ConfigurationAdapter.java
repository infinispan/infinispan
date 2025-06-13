package org.infinispan.jcache.embedded;

import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.Factory;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * ConfigurationAdapter takes {@link javax.cache.configuration.Configuration} and creates
 * equivalent instance of {@link org.infinispan.configuration.cache.Configuration}
 *
 * @author Vladimir Blagojevic
 * @author Galder Zamarreño
 * @since 5.3
 */
public class ConfigurationAdapter<K, V> {

   private final GlobalConfiguration global;
   private final MutableConfiguration<K, V> c;

   private ConfigurationAdapter(GlobalConfiguration global, MutableConfiguration<K, V> configuration) {
      this.global = global;
      this.c = configuration;
   }

   public MutableConfiguration<K, V> getConfiguration() {
      return c;
   }

   public org.infinispan.configuration.cache.Configuration build() {
      return build(new ConfigurationBuilder());
   }

   public org.infinispan.configuration.cache.Configuration build(org.infinispan.configuration.cache.Configuration baseConfig) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      if (baseConfig != null) {
         builder.read(baseConfig);
      }
      return build(builder);
   }

   private org.infinispan.configuration.cache.Configuration build(ConfigurationBuilder cb) {
      if (c.isStoreByValue()) {
         Marshaller marshaller = global.serialization().marshaller();
         cb.memory().storage(StorageType.HEAP).encoding().mediaType(marshaller != null ? marshaller.mediaType() : MediaType.APPLICATION_PROTOSTREAM);
      }

      Factory<CacheLoader<K, V>> cacheLoaderFactory = c.getCacheLoaderFactory();
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
         cb.statistics().enable();

      return cb.build();
   }

   public static <K, V> ConfigurationAdapter<K, V> create(GlobalConfiguration global, Configuration<K, V> c) {
      // A configuration copy as required by the spec
      if (c instanceof CompleteConfiguration) {
         return new ConfigurationAdapter<>(global,
               new MutableConfiguration<>((CompleteConfiguration<K, V>) c));
      } else {
         //support use of Basic Configuration
         MutableConfiguration<K, V> mutableConfiguration = new MutableConfiguration<>();
         mutableConfiguration.setStoreByValue(c.isStoreByValue());
         mutableConfiguration.setTypes(c.getKeyType(), c.getValueType());
         return new ConfigurationAdapter<>(global,
               new MutableConfiguration<>(mutableConfiguration));
      }
   }

   public static <K, V> ConfigurationAdapter<K, V> create(GlobalConfiguration global) {
      return new ConfigurationAdapter<>(global, new MutableConfiguration<>());
   }


}
