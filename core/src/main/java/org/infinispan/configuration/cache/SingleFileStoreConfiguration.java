package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.persistence.file.SingleFileStore;

import java.util.Properties;

/**
 * Defines the configuration for the single file cache store.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
@BuiltBy(SingleFileStoreConfigurationBuilder.class)
@ConfigurationFor(SingleFileStore.class)
public class SingleFileStoreConfiguration extends AbstractStoreConfiguration {

   private final String location;

   private final int maxEntries;

   public SingleFileStoreConfiguration(boolean purgeOnStartup, boolean fetchPersistentState,
                                       boolean ignoreModifications, AsyncStoreConfiguration async,
                                       SingletonStoreConfiguration singletonStore, boolean preload, boolean shared,
                                       Properties properties, String location, int maxEntries) {
      super(purgeOnStartup, fetchPersistentState, ignoreModifications, async, singletonStore, preload, shared, properties);
      this.location = location;
      this.maxEntries = maxEntries;
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

      SingleFileStoreConfiguration that = (SingleFileStoreConfiguration) o;

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
      return "SingleFileStoreConfiguration{" +
            "location='" + location + '\'' +
            ", maxEntries=" + maxEntries +
            '}';
   }

}
