package org.infinispan.configuration.cache;

import org.infinispan.loaders.CacheLoader;
import org.infinispan.util.TypedProperties;

/**
 * Configuration a specific cache loader or cache store
 * @author pmuir
 *
 */
public class LoaderConfiguration extends AbstractLoaderConfiguration {

   private final CacheLoader cacheLoader;

   LoaderConfiguration(TypedProperties properties, CacheLoader cacheLoader, boolean fetchPersistentState,
         boolean ignoreModifications, boolean purgeOnStartup, int purgerThreads, boolean purgeSynchronously,
         AsyncLoaderConfiguration async, SingletonStoreConfiguration singletonStore) {
      super(purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState,
            ignoreModifications, properties, async, singletonStore);
      this.cacheLoader = cacheLoader;
   }

   public CacheLoader cacheLoader() {
      return cacheLoader;
   }

   @Override
   public String toString() {
      return "LoaderConfiguration{" +
            "cacheLoader=" + cacheLoader +
            ", purgeOnStartup=" + purgeOnStartup() +
            ", purgeSynchronously=" + purgeSynchronously() +
            ", purgerThreads=" + purgerThreads() +
            ", fetchPersistentState=" + fetchPersistentState() +
            ", ignoreModifications=" + ignoreModifications() +
            ", properties=" + properties() +
            ", async=" + async() +
            ", singletonStore=" + singletonStore() +
            '}';
   }

}
