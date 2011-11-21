package org.infinispan.configuration.cache;

import org.infinispan.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.loaders.CacheLoader;
import org.infinispan.util.TypedProperties;

public class LoaderConfiguration extends AbstractTypedPropertiesConfiguration {

   private final CacheLoader cacheLoader;
   private final boolean fetchPersistentState;
   private final boolean ignoreModifications;
   private final boolean purgeOnStartup;
   private final int purgerThreads;
   private final boolean purgeSynchronously;
   private final AsyncLoaderConfiguration async;
   private final SingletonStoreConfiguration singletonStore;

   LoaderConfiguration(TypedProperties properties, CacheLoader cacheLoader, boolean fetchPersistentState,
         boolean ignoreModifications, boolean purgeOnStartup, int purgerThreads, boolean purgeSynchronously,
         AsyncLoaderConfiguration async, SingletonStoreConfiguration singletonStore) {
      super(properties);
      this.cacheLoader = cacheLoader;
      this.fetchPersistentState = fetchPersistentState;
      this.ignoreModifications = ignoreModifications;
      this.purgeOnStartup = purgeOnStartup;
      this.purgerThreads = purgerThreads;
      this.purgeSynchronously = purgeSynchronously;
      this.async = async;
      this.singletonStore = singletonStore;
   }

   public CacheLoader cacheLoader() {
      return cacheLoader;
   }

   public boolean fetchPersistentState() {
      return fetchPersistentState;
   }

   public boolean ignoreModifications() {
      return ignoreModifications;
   }

   public boolean purgeOnStartup() {
      return purgeOnStartup;
   }

   public int purgerThreads() {
      return purgerThreads;
   }

   public boolean purgeSynchronously() {
      return purgeSynchronously;
   }

   public AsyncLoaderConfiguration async() {
      return async;
   }

   public SingletonStoreConfiguration singletonStore() {
      return singletonStore;
   }

}
