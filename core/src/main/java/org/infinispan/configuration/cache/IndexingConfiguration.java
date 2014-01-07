package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.commons.util.TypedProperties;

/**
 * Configures indexing of entries in the cache for searching.
 */
public class IndexingConfiguration extends AbstractTypedPropertiesConfiguration {

   private final Index index;

   IndexingConfiguration(TypedProperties properties, Index index) {
      super(properties);
      this.index = index;
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

      return true;
   }

   @Override
   public int hashCode() {
      return 31 * index.hashCode();
   }

}
