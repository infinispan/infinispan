package org.infinispan.configuration.cache;

import org.infinispan.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.util.TypedProperties;

public abstract class AbstractLoaderConfiguration extends AbstractTypedPropertiesConfiguration {

   private final boolean purgeOnStartup;
   private final boolean purgeSynchronously;
   private boolean fetchPersistentState;
   private boolean ignoreModifications;
   
   private final AsyncLoaderConfiguration async;
   private final SingletonStoreConfiguration singletonStore;
   
   AbstractLoaderConfiguration(boolean purgeOnStartup, boolean purgeSynchronously, boolean fetchPersistentState,
         boolean ignoreModifications, TypedProperties properties, AsyncLoaderConfiguration async, SingletonStoreConfiguration singletonStore) {
      super(properties);
      this.purgeOnStartup = purgeOnStartup;
      this.purgeSynchronously = purgeSynchronously;
      this.fetchPersistentState = fetchPersistentState;
      this.ignoreModifications = ignoreModifications;
      this.async = async;
      this.singletonStore = singletonStore;
   }

   public AsyncLoaderConfiguration async() {
      return async;
   }
   
   public SingletonStoreConfiguration singletonStore() {
      return singletonStore;
   }

   public boolean purgeOnStartup() {
      return purgeOnStartup;
   }

   public boolean purgeSynchronously() {
      return purgeSynchronously;
   }

   public boolean fetchPersistentState() {
      return fetchPersistentState;
   }

   public boolean ignoreModifications() {
      return ignoreModifications;
   }

}
