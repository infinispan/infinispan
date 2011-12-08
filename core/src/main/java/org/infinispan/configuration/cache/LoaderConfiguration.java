package org.infinispan.configuration.cache;

import org.infinispan.loaders.CacheLoader;
import org.infinispan.util.TypedProperties;

public class LoaderConfiguration extends AbstractLoaderConfiguration {

   private final CacheLoader cacheLoader;
   private final int purgerThreads;

   LoaderConfiguration(TypedProperties properties, CacheLoader cacheLoader, boolean fetchPersistentState,
         boolean ignoreModifications, boolean purgeOnStartup, int purgerThreads, boolean purgeSynchronously,
         AsyncLoaderConfiguration async, SingletonStoreConfiguration singletonStore) {
      super(purgeOnStartup, purgeSynchronously, fetchPersistentState, ignoreModifications, properties, async, singletonStore);
      this.cacheLoader = cacheLoader;
      this.purgerThreads = purgerThreads;
   }

   public CacheLoader cacheLoader() {
      return cacheLoader;
   }

   public int purgerThreads() {
      return purgerThreads;
   }

}
