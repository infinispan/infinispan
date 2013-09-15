package org.infinispan.compatibility.adaptor52x;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.loaders.CacheLoader;

import java.util.Properties;

/**
 * @author Mircea Markus
 * @since 6.0
 */
@BuiltBy(Adaptor52xStoreConfigurationBuilder.class)
@ConfigurationFor(Adaptor52xStore.class)
public class Adaptor52xStoreConfiguration extends AbstractStoreConfiguration {
   private final CacheLoader loader;

   public Adaptor52xStoreConfiguration(boolean purgeOnStartup, boolean fetchPersistentState, boolean ignoreModifications, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore, boolean preload, boolean shared, Properties properties, CacheLoader loader) {
      super(purgeOnStartup, fetchPersistentState, ignoreModifications, async, singletonStore, preload, shared, properties);
      this.loader = loader;
   }

   public CacheLoader getLoader() {
      return loader;
   }

   @Override
   public String toString() {
      return "Adaptor52xStoreConfiguration{" +   super.toString() +
            "loader=" + loader +
            '}';
   }
}
