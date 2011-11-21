package org.infinispan.configuration.cache;

import org.infinispan.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.util.TypedProperties;

/**
 * Configures indexing of entries in the cache for searching.
 */
public class IndexingConfiguration extends AbstractTypedPropertiesConfiguration {

   private final boolean enabled;
   private final boolean indexLocalOnly;

   IndexingConfiguration(TypedProperties properties, boolean enabled, boolean indexLocalOnly) {
      super(properties);
      this.enabled = enabled;
      this.indexLocalOnly = indexLocalOnly;
   }

   /**
    * Whether indexing is enabled. False by default.
    */
   public boolean isEnabled() {
      return enabled;
   }

   /**
    * If true, only index changes made locally, ignoring remote changes. This is useful if indexes
    * are shared across a cluster to prevent redundant indexing of updates.
    */
   public boolean isIndexLocalOnly() {
      return indexLocalOnly;
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
   public TypedProperties getProperties() {
      // Overridden to replace Javadoc
      return super.getProperties();
   }

}
