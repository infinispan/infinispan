package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.commons.util.TypedProperties;

/**
 * Configures indexing of entries in the cache for searching.
 */
public class IndexingConfiguration extends AbstractTypedPropertiesConfiguration {

   private final Index index;
   private final boolean autoConfig;
   private static final String DIRECTORY_PROVIDER_KEY = "directory_provider";
   private static final String RAM_DIRECTORY_PROVIDER = "ram";

   IndexingConfiguration(TypedProperties properties, Index index, boolean autoConfig) {
      super(properties);
      this.index = index;
      this.autoConfig = autoConfig;
   }

   /**
    * Whether indexing is enabled. False by default.
    *
    * @deprecated Use {@link #index()} instead
    */
   @Deprecated
   public boolean enabled() {
      return index.isEnabled();
   }

   /**
    * If true, only index changes made locally, ignoring remote changes. This is useful if indexes
    * are shared across a cluster to prevent redundant indexing of updates.
    *
    * @deprecated Use {@link #index()} instead
    */
   @Deprecated
   public boolean indexLocalOnly() {
      return index.isLocalOnly();
   }

   /**
    * <p>
    * These properties are passed directly to the embedded Hibernate Search engine, so for the
    * complete and up to date documentation about available properties refer to the Hibernate Search
    * reference of the version you're using with Infinispan Query.
    * </p>
    * 
    * @see <a
    *      href="http://docs.jboss.org/hibernate/stable/search/reference/en-US/html_single/">Hibernate
    *      Search</a>
    */
   @Override
   public TypedProperties properties() {
      // Overridden to replace Javadoc
      return super.properties();
   }

   /**
    * Returns the indexing mode of this cache.
    */
   public Index index() {
      return index;
   }

   /**
    * Determines if autoconfig is enabled for this IndexingConfiguration
    */
   public boolean autoConfig() {
      return autoConfig;
   }

   /**
    * Check if the indexes can be shared. Currently only "ram"
    * based indexes don't allow any sort of sharing
    * 
    * @return false if the index is ram only and thus not shared
    */
   public boolean indexShareable() {
      TypedProperties properties = properties();
      boolean hasRamDirectoryProvider = false;
      for (Object objKey : properties.keySet()) {
         String key = (String) objKey;
         if (key.endsWith(DIRECTORY_PROVIDER_KEY)) {
            if (properties.get(key).equals(RAM_DIRECTORY_PROVIDER)) {
               hasRamDirectoryProvider = true;
            } else {
               return true;
            }
         }
      }
      return !hasRamDirectoryProvider;
   }
   
   @Override
   public String toString() {
      return "IndexingConfiguration{" +
            "index=" + index +
            ", properties=" + properties() +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      IndexingConfiguration that = (IndexingConfiguration) o;

      if (index != that.index) return false;
      if (autoConfig != that.autoConfig) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (index != null ? index.hashCode() : 0);
      result = 31 * result + (autoConfig ? 1 : 0);
      return result;
   }

}
