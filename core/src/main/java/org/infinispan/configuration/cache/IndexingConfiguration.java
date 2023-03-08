package org.infinispan.configuration.cache;

import static org.infinispan.commons.configuration.attributes.CollectionAttributeCopier.collectionCopier;
import static org.infinispan.commons.util.Immutables.immutableTypedProperties;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.parsing.Element;

/**
 * Configures indexing of entries in the cache for searching.
 */
public class IndexingConfiguration extends ConfigurationElement<IndexingConfiguration> {
   /**
    * @deprecated since 11.0
    */
   @Deprecated
   public static final AttributeDefinition<Index> INDEX = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.INDEX, null, Index.class).immutable().build();
   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.ENABLED, false).immutable().build();
   /**
    * @deprecated since 11.0
    */
   @Deprecated
   public static final AttributeDefinition<Boolean> AUTO_CONFIG = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.AUTO_CONFIG, false).immutable().build();
   public static final AttributeDefinition<Map<Class<?>, Class<?>>> KEY_TRANSFORMERS = AttributeDefinition.builder(Element.KEY_TRANSFORMERS, null, (Class<Map<Class<?>, Class<?>>>) (Class<?>) Map.class)
         .copier(collectionCopier())
         .initializer(HashMap::new).immutable().build();
   public static final AttributeDefinition<Set<String>> INDEXED_ENTITIES = AttributeDefinition.builder(Element.INDEXED_ENTITIES, null, (Class<Set<String>>) (Class<?>) Set.class)
         .copier(collectionCopier())
         .initializer(HashSet::new).immutable().build();
   public static final AttributeDefinition<IndexStorage> STORAGE = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.STORAGE, IndexStorage.FILESYSTEM)
         .immutable().build();
   public static final AttributeDefinition<IndexStartupMode> STARTUP_MODE = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.STARTUP_MODE, IndexStartupMode.NONE)
         .immutable().build();
   public static final AttributeDefinition<String> PATH = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.PATH, null, String.class).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(IndexingConfiguration.class, AbstractTypedPropertiesConfiguration.attributeSet(), INDEX, AUTO_CONFIG, KEY_TRANSFORMERS, INDEXED_ENTITIES, ENABLED, STORAGE, STARTUP_MODE, PATH);
   }

   private final Attribute<TypedProperties> properties;
   private final Set<Class<?>> resolvedIndexedClasses;
   private final IndexReaderConfiguration readerConfiguration;
   private final IndexWriterConfiguration writerConfiguration;

   IndexingConfiguration(AttributeSet attributes, Set<Class<?>> resolvedIndexedClasses,
                         IndexReaderConfiguration readerConfiguration, IndexWriterConfiguration writerConfiguration) {
      super(Element.INDEXING, attributes);
      this.readerConfiguration = readerConfiguration;
      this.writerConfiguration = writerConfiguration;
      this.resolvedIndexedClasses = resolvedIndexedClasses;
      this.properties = this.attributes.attribute(AbstractTypedPropertiesConfiguration.PROPERTIES);
      if (properties.isModified()) {
         properties.set(immutableTypedProperties(properties.get()));
      }
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
    * @deprecated Since 12.0, indexing behaviour is defined by {@link #writer()} and {@link #reader()}.
    */
   @Deprecated
   public TypedProperties properties() {
      return properties.get();
   }

   /**
    * Returns the indexing mode of this cache.
    *
    * @deprecated Since 11. This configuration will be removed in next major version as the index mode is calculated
    * automatically.
    */
   @Deprecated
   public Index index() {
      return attributes.attribute(INDEX).get();
   }

   /**
    * Determines if indexing is enabled for this cache configuration.
    */
   public boolean enabled() {
      return attributes.attribute(ENABLED).get();
   }

   /**
    * Determines if autoconfig is enabled for this IndexingConfiguration.
    * @deprecated Since 11.0, with no replacement.
    */
   @Deprecated
   public boolean autoConfig() {
      return attributes.attribute(AUTO_CONFIG).get();
   }

   public IndexStorage storage() {
      return attributes.attribute(STORAGE).get();
   }

   public IndexStartupMode startupMode() {
      return attributes.attribute(STARTUP_MODE).get();
   }

   public String path() {
      return attributes.attribute(PATH).get();
   }

   /**
    * The currently configured key transformers.
    *
    * @return a {@link Map} in which the map key is the key class and the value is the Transformer class.
    */
   public Map<Class<?>, Class<?>> keyTransformers() {
      return attributes.attribute(KEY_TRANSFORMERS).get();
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
      return attributes.attribute(INDEXED_ENTITIES).get();
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

   public IndexReaderConfiguration reader() {
      return readerConfiguration;
   }

   public IndexWriterConfiguration writer() {
      return writerConfiguration;
   }

   /**
    * Does the index use a provider that does not persist upon restart?
    */
   public boolean isVolatile() {
      return attributes.attribute(STORAGE).get().equals(IndexStorage.LOCAL_HEAP);
   }

   @Override
   public String toString() {
      return attributes.toString(null) +
            ", reader=" + readerConfiguration +
            ", writer=" + writerConfiguration;
   }
}
