package org.jboss.as.clustering.infinispan.subsystem;

import java.util.Properties;

import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;

/**
 * @author Mircea Markus
 * @since 6.0
 */
@ConfigurationFor(CustomCacheLoader.class)
public class CustomCacheLoaderConfiguration extends AbstractStoreConfiguration {

   private String location;

   public CustomCacheLoaderConfiguration(boolean purgeOnStartup, boolean fetchPersistentState, boolean ignoreModifications,
                                         AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore,
                                         boolean preload, boolean shared, Properties properties, String someProperty) {
      super(purgeOnStartup, fetchPersistentState, ignoreModifications, async, singletonStore, preload, shared, properties);
      this.location = someProperty;
   }

   public String someProperty() {
      return location;
   }
}
