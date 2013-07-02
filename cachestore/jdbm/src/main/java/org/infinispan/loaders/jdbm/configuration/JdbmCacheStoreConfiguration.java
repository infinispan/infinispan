package org.infinispan.loaders.jdbm.configuration;

import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.LegacyConfigurationAdaptor;
import org.infinispan.configuration.cache.LegacyLoaderAdapter;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.loaders.jdbm.JdbmCacheStoreConfig;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.util.TypedProperties;

@BuiltBy(JdbmCacheStoreConfigurationBuilder.class)
public class JdbmCacheStoreConfiguration extends AbstractStoreConfiguration implements
      LegacyLoaderAdapter<JdbmCacheStoreConfig> {
   private final String comparatorClassName;
   private final int expiryQueueSize;
   private final String location;

   public JdbmCacheStoreConfiguration(String comparatorClassName, int expiryQueueSize, String location, boolean purgeOnStartup, boolean purgeSynchronously, int purgerThreads,
         boolean fetchPersistentState, boolean ignoreModifications, TypedProperties properties,
         AsyncStoreConfiguration asyncStoreConfiguration, SingletonStoreConfiguration singletonStoreConfiguration) {
      super(purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState, ignoreModifications, properties,
            asyncStoreConfiguration, singletonStoreConfiguration);
      this.comparatorClassName = comparatorClassName;
      this.expiryQueueSize = expiryQueueSize;
      this.location = location;
   }

   public String comparatorClassName() {
      return comparatorClassName;
   }

   public int expiryQueueSize() {
      return expiryQueueSize;
   }

   public String location() {
      return location;
   }

   @Override
   public JdbmCacheStoreConfig adapt() {
      JdbmCacheStoreConfig config = new JdbmCacheStoreConfig();

      LegacyConfigurationAdaptor.adapt(this, config);

      config.setComparatorClassName(comparatorClassName);
      config.setExpiryQueueSize(expiryQueueSize);
      config.setLocation(location);

      return config;
   }

}
