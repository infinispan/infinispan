package org.infinispan.configuration.cache;

import static org.infinispan.configuration.parsing.Element.INDEXING;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.CollectionAttributeCopier;
import org.infinispan.commons.configuration.attributes.Matchable;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.commons.util.TypedProperties;

/**
 * Configures indexing of entries in the cache for searching.
 */
public class IndexingConfiguration extends AbstractTypedPropertiesConfiguration implements Matchable<IndexingConfiguration>, ConfigurationInfo {
   /**
    * @deprecated since 11.0
    */
   @Deprecated
   public static final AttributeDefinition<Index> INDEX = AttributeDefinition.builder("index", null, Index.class).immutable().build();
   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).immutable().build();
   /**
    * @deprecated since 11.0
    */
   @Deprecated
   public static final AttributeDefinition<Boolean> AUTO_CONFIG = AttributeDefinition.builder("autoConfig", false).immutable().build();
   public static final AttributeDefinition<Map<Class<?>, Class<?>>> KEY_TRANSFORMERS = AttributeDefinition.builder("key-transformers", null, (Class<Map<Class<?>, Class<?>>>) (Class<?>) Map.class)
         .copier(CollectionAttributeCopier.INSTANCE)
         .initializer(HashMap::new).immutable().build();
   public static final AttributeDefinition<Set<String>> INDEXED_ENTITIES = AttributeDefinition.builder("indexed-entities", null, (Class<Set<String>>) (Class<?>) Set.class)
         .copier(CollectionAttributeCopier.INSTANCE)
         .initializer(HashSet::new).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(IndexingConfiguration.class, AbstractTypedPropertiesConfiguration.attributeSet(), INDEX, AUTO_CONFIG, KEY_TRANSFORMERS, INDEXED_ENTITIES, ENABLED);
   }

   static final ElementDefinition<IndexingConfiguration> ELEMENT_DEFINITION = new DefaultElementDefinition<>(INDEXING.getLocalName());

   /**
    * @deprecated since 11.0
    */
   @Deprecated
   private final Attribute<Index> index;

   /**
    * @deprecated since 11.0
    */
   @Deprecated
   private final Attribute<Boolean> autoConfig;

   private final Attribute<Map<Class<?>, Class<?>>> keyTransformers;
   private final Attribute<Set<String>> indexedEntities;
   private final Set<Class<?>> resolvedIndexedClasses;
   private final Attribute<Boolean> enabled;
   private final boolean isVolatile;

   IndexingConfiguration(AttributeSet attributes, boolean isVolatile, Set<Class<?>> resolvedIndexedClasses) {
      super(attributes);
      this.isVolatile = isVolatile;
      this.resolvedIndexedClasses = resolvedIndexedClasses;
      index = attributes.attribute(INDEX);
      autoConfig = attributes.attribute(AUTO_CONFIG);
      keyTransformers = attributes.attribute(KEY_TRANSFORMERS);
      indexedEntities = attributes.attribute(INDEXED_ENTITIES);
      enabled = attributes.attribute(ENABLED);
   }

   @Override
   public ElementDefinition<IndexingConfiguration> getElementDefinition() {
      return ELEMENT_DEFINITION;
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
    *
    * @deprecated Since 11. This configuration will be removed in next major version as the index mode is calculated
    * automatically.
    */
   @Deprecated
   public Index index() {
      return index.get();
   }

   /**
    * Determines if indexing is enabled for this cache configuration.
    */
   public boolean enabled() {
      return enabled.get();
   }

   /**
    * Determines if autoconfig is enabled for this IndexingConfiguration.
    * @deprecated Since 11.0, with no replacement.
    */
   @Deprecated
   public boolean autoConfig() {
      return autoConfig.get();
   }

   /**
    * The currently configured key transformers.
    *
    * @return a {@link Map} in which the map key is the key class and the value is the Transformer class.
    */
   public Map<Class<?>, Class<?>> keyTransformers() {
      return keyTransformers.get();
   }

   /**
    * The subset of indexed entity classes. This does not include the protobuf types. For the entire set of types use
    * {@link #indexedEntityTypes()}.
    *
    * @deprecated since 11. Usages should be converted to {@link #indexedEntityTypes()} as this method will be removed
    * in next major version.
    */
   @Deprecated
   public Set<Class<?>> indexedEntities() {
      return resolvedIndexedClasses;
   }

   /**
    * The set of fully qualified names of indexed entity types, either Java classes or protobuf type names. This
    * configuration corresponds to the {@code <indexed-entities>} XML configuration element.
    */
   public Set<String> indexedEntityTypes() {
      return indexedEntities.get();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   /**
    * Check if the indexes can be shared. Currently no index can be shared, so it always returns false. sharing.
    *
    * @return always false, starting with version 11.0
    * @deprecated Since 11.0 with no replacement; to be removed in next major version.
    */
   @Deprecated
   public final boolean indexShareable() {
      return false;
   }

   /**
    * Does the index use a provider that does not persist upon restart?
    */
   public boolean isVolatile() {
      return isVolatile;
   }

   @Override
   public String toString() {
      return "IndexingConfiguration [attributes=" + attributes + "]";
   }
}
