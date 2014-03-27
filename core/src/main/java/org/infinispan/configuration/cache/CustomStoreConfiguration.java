package org.infinispan.configuration.cache;

import java.util.Properties;

public class CustomStoreConfiguration extends AbstractStoreConfiguration {
   private final Class<?> customStoreClass;

   public CustomStoreConfiguration(Class<?> customStoreClass, boolean purgeOnStartup, boolean fetchPersistentState, boolean ignoreModifications,
                                   AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore, boolean preload,
                                   boolean shared, Properties properties) {
      super(purgeOnStartup, fetchPersistentState, ignoreModifications, async, singletonStore, preload, shared, properties);
      this.customStoreClass = customStoreClass;
   }

   public Class<?> customStoreClass() {
      return customStoreClass;
   }

}
