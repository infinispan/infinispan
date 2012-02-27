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

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      LoaderConfiguration that = (LoaderConfiguration) o;

      if (cacheLoader != null ? !cacheLoader.equals(that.cacheLoader) : that.cacheLoader != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (cacheLoader != null ? cacheLoader.hashCode() : 0);
      return result;
   }

}
