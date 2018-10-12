package org.infinispan.configuration.cache;

import static org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration.PROPERTIES;
import static org.infinispan.configuration.cache.IndexingConfiguration.AUTO_CONFIG;
import static org.infinispan.configuration.cache.IndexingConfiguration.INDEX;
import static org.infinispan.configuration.cache.IndexingConfiguration.INDEXED_ENTITIES;
import static org.infinispan.configuration.cache.IndexingConfiguration.KEY_TRANSFORMERS;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Configures indexing of entries in the cache for searching.
 */
public class IndexingConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<IndexingConfiguration> {

   private static final Log log = LogFactory.getLog(IndexingConfigurationBuilder.class);

   private final AttributeSet attributes;

   IndexingConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      attributes = IndexingConfiguration.attributeDefinitionSet();
   }

   /**
    * Enable indexing.
    *
    * @deprecated Use {@link #index(Index)} instead
    */
   @Deprecated
   public IndexingConfigurationBuilder enable() {
      Attribute<Index> index = attributes.attribute(INDEX);
      if (index.get() == Index.NONE)
         index.set(Index.ALL);
      return this;
   }

   /**
    * Disable indexing.
    *
    * @deprecated Use {@link #index(Index)} instead
    */
   @Deprecated
   public IndexingConfigurationBuilder disable() {
      attributes.attribute(INDEX).set(Index.NONE);
      return this;
   }

   /**
    * Enable or disable indexing.
    *
    * @deprecated Use {@link #index(Index)} instead
    */
   @Deprecated
   public IndexingConfigurationBuilder enabled(boolean enabled) {
      Attribute<Index> index = attributes.attribute(INDEX);
      if (index.get() == Index.NONE & enabled)
         index.set(Index.ALL);
      else if (!enabled)
         index.set(Index.NONE);
      return this;
   }

   boolean enabled() {
      return attributes.attribute(INDEX).get().isEnabled();
   }

   /**
    * If true, only index changes made locally, ignoring remote changes. This is useful if indexes
    * are shared across a cluster to prevent redundant indexing of updates.
    * @deprecated Use {@link #index(Index)} instead
    */
   @Deprecated
   public IndexingConfigurationBuilder indexLocalOnly(boolean b) {
      if (b)
         attributes.attribute(INDEX).set(Index.LOCAL);

      return this;
   }

   @Deprecated
   boolean indexLocalOnly() {
      return attributes.attribute(INDEX).get().isLocalOnly();
   }

   /**
    * Registers a transformer for a key class.
    *
    * @param keyClass the class of the key
    * @param keyTransformerClass the class of the org.infinispan.query.Transformer that handles this key type
    * @return <code>this</code>, for method chaining
    */
   public IndexingConfigurationBuilder addKeyTransformer(Class<?> keyClass, Class<?> keyTransformerClass) {
      Map<Class<?>, Class<?>> indexedEntities = keyTransformers();
      indexedEntities.put(keyClass, keyTransformerClass);
      attributes.attribute(KEY_TRANSFORMERS).set(indexedEntities);
      return this;
   }

   private Map<Class<?>, Class<?>> keyTransformers() {
      return attributes.attribute(KEY_TRANSFORMERS).get();
   }

   /**
    * Defines a single property. Can be used multiple times to define all needed properties, but the
    * full set is overridden by {@link #withProperties(Properties)}.
    * <p>
    * These properties are passed directly to the embedded Hibernate Search engine, so for the
    * complete and up to date documentation about available properties refer to the the Hibernate Search
    * reference of the version used by Infinispan Query.
    *
    * @see <a href="http://docs.jboss.org/hibernate/stable/search/reference/en-US/html_single/">Hibernate Search</a>
    * @param key Property key
    * @param value Property value
    * @return <code>this</code>, for method chaining
    */
   public IndexingConfigurationBuilder addProperty(String key, String value) {
      return setProperty(key, value);
   }

   /**
    * Defines a single value. Can be used multiple times to define all needed property values, but the
    * full set is overridden by {@link #withProperties(Properties)}.
    * <p>
    * These properties are passed directly to the embedded Hibernate Search engine, so for the
    * complete and up to date documentation about available properties refer to the the Hibernate Search
    * reference of the version used by Infinispan Query.
    *
    * @see <a href="http://docs.jboss.org/hibernate/stable/search/reference/en-US/html_single/">Hibernate Search</a>
    * @param key Property key
    * @param value Property value
    * @return <code>this</code>, for method chaining
    */
   public IndexingConfigurationBuilder setProperty(String key, Object value) {
      TypedProperties properties = attributes.attribute(PROPERTIES).get();
      properties.put(key, value);
      attributes.attribute(PROPERTIES).set(properties);
      return this;
   }

   /**
    * The Query engine relies on properties for configuration.
    * <p>
    * These properties are passed directly to the embedded Hibernate Search engine, so for the
    * complete and up to date documentation about available properties refer to the Hibernate Search
    * reference of the version you're using with Infinispan Query.
    *
    * @see <a href="http://docs.jboss.org/hibernate/stable/search/reference/en-US/html_single/">Hibernate Search</a>
    * @param props the properties
    * @return <code>this</code>, for method chaining
    */
   public IndexingConfigurationBuilder withProperties(Properties props) {
      attributes.attribute(PROPERTIES).set(TypedProperties.toTypedProperties(props));
      return this;
   }

   /**
    * Indicates indexing mode
    */
   public IndexingConfigurationBuilder index(Index index) {
      attributes.attribute(INDEX).set(index);
      return this;
   }

   /**
    * When enabled, applies to properties default configurations based on
    * the cache type
    *
    * @param autoConfig boolean
    * @return <code>this</code>, for method chaining
    */
   public IndexingConfigurationBuilder autoConfig(boolean autoConfig) {
      attributes.attribute(AUTO_CONFIG).set(autoConfig);
      return this;
   }

   public boolean autoConfig() {
      return attributes.attribute(AUTO_CONFIG).get();
   }

   public IndexingConfigurationBuilder addIndexedEntity(Class<?> indexedEntity) {
      Set<Class<?>> indexedEntities = indexedEntities();
      indexedEntities.add(indexedEntity);
      attributes.attribute(INDEXED_ENTITIES).set(indexedEntities);
      return this;
   }

   private Set<Class<?>> indexedEntities() {
      return attributes.attribute(INDEXED_ENTITIES).get();
   }

   @Override
   public void validate() {
      if (enabled()) {
         //Indexing is not conceptually compatible with Invalidation mode
         if (clustering().cacheMode().isInvalidation()) {
            throw log.invalidConfigurationIndexingWithInvalidation();
         }
         if (indexedEntities().isEmpty() && !getBuilder().template()) {
            //TODO [anistor] This does not take into account eventual programmatically defined entity mappings
            log.noIndexableClassesDefined();
         }
         if (attributes.attribute(INDEX).get() == Index.ALL && !clustering().cacheMode().isReplicated()) {
            log.allIndexingInNonReplicatedCache();
         }
      }
      //TODO [anistor] Infinispan 10 must not allow definition of indexed entities or indexing properties if indexing is not enabled
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
      if (enabled()) {
         // Check that the query module is on the classpath.
         try {
            String clazz = "org.infinispan.query.Search";
            Util.loadClassStrict( clazz, globalConfig.classLoader() );
         } catch (ClassNotFoundException e) {
            throw log.invalidConfigurationIndexingWithoutModule();
         }
      }
   }

   @Override
   public IndexingConfiguration create() {
      TypedProperties typedProperties = attributes.attribute(PROPERTIES).get();
      if (autoConfig()) {
         if (clustering().cacheMode().isDistributed()) {
            IndexOverlay.DISTRIBUTED_INFINISPAN.apply(typedProperties );
         } else {
            IndexOverlay.NON_DISTRIBUTED_FS.apply(typedProperties);
         }
         attributes.attribute(PROPERTIES).set(typedProperties);
      }
      return new IndexingConfiguration(attributes.protect());
   }

   @Override
   public IndexingConfigurationBuilder read(IndexingConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }

   @Override
   public String toString() {
      return "IndexingConfigurationBuilder [attributes=" + attributes + "]";
   }
}
