package org.infinispan.configuration.cache;

import java.util.Properties;

import org.infinispan.util.TypedProperties;

/**
 * Configures indexing of entries in the cache for searching.
 */
public class IndexingConfigurationBuilder extends AbstractConfigurationChildBuilder<IndexingConfiguration>{

   private boolean enabled;
   private boolean indexLocalOnly;
   private Properties properties;
   
   IndexingConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * Enable indexing
    */
   public IndexingConfigurationBuilder enable() {
      this.enabled = true;
      return this;
   }
   
   /**
    * Disable indexing
    */
   public IndexingConfigurationBuilder disable() {
      this.enabled = false;
      return this;
   }
   
   public IndexingConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   /**
    * If true, only index changes made locally, ignoring remote changes. This is useful if indexes
    * are shared across a cluster to prevent redundant indexing of updates.
    */
   public IndexingConfigurationBuilder indexLocalOnly(boolean b) {
      this.indexLocalOnly = b;
      return this;
   }

   /**
    * <p>
    * Defines a single property. Can be used multiple times to define all needed properties, but the
    * full set is overridden by {@link #withProperties(Properties)}.
    * </p>
    * <p>
    * These properties are passed directly to the embedded Hibernate Search engine, so for the
    * complete and up to date documentation about available properties refer to the Hibernate Search
    * reference of the version you're using with Infinispan Query.
    * </p>
    * 
    * @see <a
    *      href="http://docs.jboss.org/hibernate/stable/search/reference/en-US/html_single/">Hibernate
    *      Search</a>
    * @param key Property key
    * @param value Property value
    * @return <code>this</code>, for method chaining
    */
   public IndexingConfigurationBuilder addProperty(String key, String value) {
      this.properties.put(key, value);
      return this;
   }

   /**
    * <p>
    * The Query engine relies on properties for configuration.
    * </p>
    * <p>
    * These properties are passed directly to the embedded Hibernate Search engine, so for the
    * complete and up to date documentation about available properties refer to the Hibernate Search
    * reference of the version you're using with Infinispan Query.
    * </p>
    * 
    * @see <a
    *      href="http://docs.jboss.org/hibernate/stable/search/reference/en-US/html_single/">Hibernate
    *      Search</a>
    * @param properties
    * @return <code>this</code>, for method chaining
    */
   public IndexingConfigurationBuilder withProperties(Properties props) {
      this.properties = props;
      return this;
   }

   @Override
   void validate() {
      // TODO Auto-generated method stub
      
   }

   @Override
   IndexingConfiguration create() {
      return new IndexingConfiguration(TypedProperties.toTypedProperties(properties), enabled, indexLocalOnly);
   }

}
