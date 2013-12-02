package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;

import java.util.Properties;

/**
 * @author Mircea Markus
 * @since 6.0
 */
@ConfigurationFor(CustomCacheWriter.class)
public class CustomCacheWriterConfiguration extends AbstractStoreConfiguration {

   private String someProperty;

   public CustomCacheWriterConfiguration(boolean purgeOnStartup, boolean fetchPersistentState, boolean ignoreModifications,
                                         AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore,
                                         boolean preload, boolean shared, Properties properties, String someProperty) {
      super(purgeOnStartup, fetchPersistentState, ignoreModifications, async, singletonStore, preload, shared, properties);
      this.someProperty = someProperty;
   }

   public String someProperty() {
      return someProperty;
   }

}
