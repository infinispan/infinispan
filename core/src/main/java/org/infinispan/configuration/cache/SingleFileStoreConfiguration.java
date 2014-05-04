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

   private final float fragmentationFactor;

   public SingleFileStoreConfiguration(boolean purgeOnStartup, boolean fetchPersistentState,
                                       boolean ignoreModifications, AsyncStoreConfiguration async,
                                       SingletonStoreConfiguration singletonStore, boolean preload, boolean shared,
                                       Properties properties, String location, int maxEntries, float fragmentationFactor) {
      super(purgeOnStartup, fetchPersistentState, ignoreModifications, async, singletonStore, preload, shared, properties);
      this.location = location;
      this.maxEntries = maxEntries;
      this.fragmentationFactor  = fragmentationFactor;
   }

   public String location() {
      return location;
   }

   public int maxEntries() {
      return maxEntries;
   }

   public float fragmentationFactor () {
      return fragmentationFactor;
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
      if (fragmentationFactor  != that.fragmentationFactor) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (location != null ? location.hashCode() : 0);
      result = 31 * result + maxEntries;
      result = 31 * result + Float.floatToIntBits(fragmentationFactor);
      return result;
   }

   @Override
   public String toString() {
      return "SingleFileStoreConfiguration{" +
            "location='" + location + '\'' +
            ", maxEntries=" + maxEntries +
            ", fragmentationFactor =" + fragmentationFactor  +
            '}';
   }

}
