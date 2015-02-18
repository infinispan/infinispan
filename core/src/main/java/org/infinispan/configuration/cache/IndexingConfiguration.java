package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.TypedProperties;

/**
 * Configures indexing of entries in the cache for searching.
 */
public class IndexingConfiguration extends AbstractTypedPropertiesConfiguration {
   public static final AttributeDefinition<Index> INDEX = AttributeDefinition.builder("index", Index.NONE).immutable().build();
   public static final AttributeDefinition<Boolean> AUTO_CONFIG = AttributeDefinition.builder("autoConfig", false).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(IndexingConfiguration.class, AbstractTypedPropertiesConfiguration.attributeSet(), INDEX, AUTO_CONFIG);
   }

   private static final String DIRECTORY_PROVIDER_KEY = "directory_provider";
   private static final String RAM_DIRECTORY_PROVIDER = "ram";

   private final Attribute<Index> index;
   private final Attribute<Boolean> autoConfig;

   public IndexingConfiguration(AttributeSet attributes) {
      super(attributes);
      index = attributes.attribute(INDEX);
      autoConfig = attributes.attribute(AUTO_CONFIG);
   }

   /**
    * Whether indexing is enabled. False by default.
    *
    * @deprecated Use {@link #index()} instead
    */
   @Deprecated
   public boolean enabled() {
      return index().isEnabled();
   }

   /**
    * If true, only index changes made locally, ignoring remote changes. This is useful if indexes
    * are shared across a cluster to prevent redundant indexing of updates.
    *
    * @deprecated Use {@link #index()} instead
    */
   @Deprecated
   public boolean indexLocalOnly() {
      return index().isLocalOnly();
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
      return index.get();
   }

   /**
    * Determines if autoconfig is enabled for this IndexingConfiguration
    */
   public boolean autoConfig() {
      return autoConfig.get();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   /**
    * Check if the indexes can be shared. Currently only "ram" based indexes don't allow any sort of
    * sharing
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
      return "IndexingConfiguration [attributes=" + attributes + "]";
   }

}
