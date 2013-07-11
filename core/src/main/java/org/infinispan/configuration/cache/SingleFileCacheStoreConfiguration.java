package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.config.parsing.XmlConfigHelper;
import org.infinispan.loaders.file.SingleFileCacheStoreConfig;

/**
 * Defines the configuration for the single file cache store.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
@BuiltBy(SingleFileCacheStoreConfigurationBuilder.class)
public class SingleFileCacheStoreConfiguration extends AbstractStoreConfiguration implements LegacyLoaderAdapter<SingleFileCacheStoreConfig> {

   private final String location;

   private final int maxEntries;

   public SingleFileCacheStoreConfiguration(String location, int maxKeysInMemory,
         boolean purgeOnStartup, boolean purgeSynchronously, int purgerThreads, boolean fetchPersistentState,
         boolean ignoreModifications, TypedProperties properties, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore) {
      super(purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState,
            ignoreModifications, properties, async, singletonStore);
      this.location = location;
      this.maxEntries = maxKeysInMemory;
   }

   public String location() {
      return location;
   }

   public int maxEntries() {
      return maxEntries;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      SingleFileCacheStoreConfiguration that = (SingleFileCacheStoreConfiguration) o;

      if (maxEntries != that.maxEntries) return false;
      if (location != null ? !location.equals(that.location) : that.location != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (location != null ? location.hashCode() : 0);
      result = 31 * result + maxEntries;
      return result;
   }

   @Override
   public String toString() {
      return "SingleFileCacheStoreConfiguration{" +
            "location='" + location + '\'' +
            ", maxEntries=" + maxEntries +
            '}';
   }

   @Override
   public SingleFileCacheStoreConfig adapt() {
      SingleFileCacheStoreConfig config = new SingleFileCacheStoreConfig();

      LegacyConfigurationAdaptor.adapt(this, config);

      config.location(location);
      config.maxEntries(maxEntries);

      XmlConfigHelper.setValues(config, properties(), false, true);
      return config;
   }

}
