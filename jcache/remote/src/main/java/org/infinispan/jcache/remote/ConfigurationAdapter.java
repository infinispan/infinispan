package org.infinispan.jcache.remote;

import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.MutableConfiguration;

public class ConfigurationAdapter<K, V> {

   private MutableConfiguration<K, V> c;

   private ConfigurationAdapter(MutableConfiguration<K, V> configuration) {
      this.c = configuration;
   }

   public MutableConfiguration<K, V> getConfiguration() {
      return c;
   }

   public static <K, V> ConfigurationAdapter<K, V> create(Configuration<K, V> c) {
      // A configuration copy as required by the spec
      if (c instanceof CompleteConfiguration) {
         return new ConfigurationAdapter<>(
               new MutableConfiguration<>((CompleteConfiguration<K, V>) c));
      } else {
         //support use of Basic Configuration
         MutableConfiguration<K, V> mutableConfiguration = new MutableConfiguration<>();
         mutableConfiguration.setStoreByValue(c.isStoreByValue());
         mutableConfiguration.setTypes(c.getKeyType(), c.getValueType());
         return new ConfigurationAdapter<>(new MutableConfiguration<>(mutableConfiguration));
      }
   }

   public static <K, V> ConfigurationAdapter<K, V> create() {
      return new ConfigurationAdapter<>(new MutableConfiguration<>());
   }
}
