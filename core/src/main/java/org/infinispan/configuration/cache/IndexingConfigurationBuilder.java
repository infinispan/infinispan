package org.infinispan.configuration.cache;

import java.util.Map;
import java.util.Properties;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.commons.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Configures indexing of entries in the cache for searching.
 */
public class IndexingConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<IndexingConfiguration>{

   private static final Log log = LogFactory.getLog(IndexingConfigurationBuilder.class);

   private Properties properties = new Properties();
   private Index index = Index.NONE;

   IndexingConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * Enable indexing
    * @deprecated Use {@link #index(Index)} instead
    */
   @Deprecated
   public IndexingConfigurationBuilder enable() {
      if (this.index == Index.NONE)
         this.index = Index.ALL;
      return this;
   }

   /**
    * Disable indexing
    * @deprecated Use {@link #index(Index)} instead
    */
   @Deprecated
   public IndexingConfigurationBuilder disable() {
      this.index = Index.NONE;
      return this;
   }

   /**
    * Enable or disable indexing
    * @deprecated Use {@link #index(Index)} instead
    */
   @Deprecated
   public IndexingConfigurationBuilder enabled(boolean enabled) {
      if (this.index == Index.NONE & enabled)
         this.index = Index.ALL;
      else if (!enabled)
         this.index = Index.NONE;
      return this;
   }

   boolean enabled() {
      return index.isEnabled();
   }

   /**
    * If true, only index changes made locally, ignoring remote changes. This is useful if indexes
    * are shared across a cluster to prevent redundant indexing of updates.
    * @deprecated Use {@link #index(Index)} instead
    */
   @Deprecated
   public IndexingConfigurationBuilder indexLocalOnly(boolean b) {
      if (b)
         this.index = Index.LOCAL;

      return this;
   }

   boolean indexLocalOnly() {
      return this.index.isLocalOnly();
   }

   /**
    * <p>
    * Defines a single property. Can be used multiple times to define all needed properties, but the
    * full set is overridden by {@link #withProperties(Properties)}.
    * </p>
    * <p>
    * These properties are passed directly to the embedded Hibernate Search engine, so for the
    * complete and up to date documentation about available properties refer to the the Hibernate Search
    * reference of the version used by Infinispan Query.
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
    * Defines a single value. Can be used multiple times to define all needed property values, but the
    * full set is overridden by {@link #withProperties(Properties)}.
    * </p>
    * <p>
    * These properties are passed directly to the embedded Hibernate Search engine, so for the
    * complete and up to date documentation about available properties refer to the the Hibernate Search
    * reference of the version used by Infinispan Query.
    * </p>
    *
    * @see <a
    *      href="http://docs.jboss.org/hibernate/stable/search/reference/en-US/html_single/">Hibernate
    *      Search</a>
    * @param key Property key
    * @param value Property value
    * @return <code>this</code>, for method chaining
    */
   public IndexingConfigurationBuilder setProperty(String key, Object value) {
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
    * @param props the properties
    * @return <code>this</code>, for method chaining
    */
   public IndexingConfigurationBuilder withProperties(Properties props) {
      this.properties = props;
      return this;
   }

   /**
    * Indicates indexing mode
    */
   public IndexingConfigurationBuilder index(Index index) {
      this.index = index;
      return this;
   }

   @Override
   public void validate() {
      if (index.isEnabled()) {
         //Indexing is not conceptually compatible with Invalidation mode
         if (clustering().cacheMode().isInvalidation()) {
            throw log.invalidConfigurationIndexingWithInvalidation();
         }
         // Check that the query module is on the classpath.
         try {
            String clazz = "org.infinispan.query.Search";
            Util.loadClassStrict( clazz, getClass().getClassLoader() );
         } catch (ClassNotFoundException e) {
            throw log.invalidConfigurationIndexingWithoutModule();
         }
      }
   }

   @Override
   public IndexingConfiguration create() {
      return new IndexingConfiguration(TypedProperties.toTypedProperties(properties), index);
   }

   @Override
   public IndexingConfigurationBuilder read(IndexingConfiguration template) {
      this.index = template.index();
      this.properties = new Properties();

      TypedProperties templateProperties = template.properties();
      if (templateProperties != null) {
         for (Map.Entry entry : templateProperties.entrySet()) {
            properties.put(entry.getKey(), entry.getValue());
         }
      }

      return this;
   }

   @Override
   public String toString() {
      return "IndexingConfigurationBuilder{" +
            "index=" + index +
            ", properties=" + properties +
            '}';
   }

}
