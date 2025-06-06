package org.infinispan.configuration.cache;

import static org.infinispan.commons.configuration.attributes.CollectionAttributeCopier.collectionCopier;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Element;

/**
 * Configures indexing of entries in the cache for searching.
 */
public class IndexingConfiguration extends ConfigurationElement<IndexingConfiguration> {

   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.ENABLED, false).immutable().build();

   public static final AttributeDefinition<Map<Class<?>, Class<?>>> KEY_TRANSFORMERS = AttributeDefinition.builder(Element.KEY_TRANSFORMERS, null, (Class<Map<Class<?>, Class<?>>>) (Class<?>) Map.class)
         .copier(collectionCopier())
         .initializer(HashMap::new).immutable().build();
   public static final AttributeDefinition<Set<String>> INDEXED_ENTITIES = AttributeDefinition.builder(Element.INDEXED_ENTITIES, null, (Class<Set<String>>) (Class<?>) Set.class)
         .copier(collectionCopier())
         .initializer(HashSet::new).build();
   public static final AttributeDefinition<IndexStorage> STORAGE = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.STORAGE, IndexStorage.FILESYSTEM)
         .immutable().build();
   public static final AttributeDefinition<IndexStartupMode> STARTUP_MODE = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.STARTUP_MODE, IndexStartupMode.AUTO)
         .immutable().build();
   public static final AttributeDefinition<String> PATH = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.PATH, null, String.class).immutable().build();
   public static final AttributeDefinition<IndexingMode> INDEXING_MODE = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.INDEXING_MODE, IndexingMode.AUTO)
         .immutable().build();
   public static final AttributeDefinition<Boolean> USE_JAVA_EMBEDDED_ENTITIES = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.USE_JAVA_EMBEDDED_ENTITIES, false).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(IndexingConfiguration.class, AbstractTypedPropertiesConfiguration.attributeSet(),
            KEY_TRANSFORMERS, INDEXED_ENTITIES, ENABLED, STORAGE, STARTUP_MODE, PATH, INDEXING_MODE, USE_JAVA_EMBEDDED_ENTITIES);
   }

   private final IndexReaderConfiguration readerConfiguration;
   private final IndexWriterConfiguration writerConfiguration;
   private final IndexShardingConfiguration shardingConfiguration;

   IndexingConfiguration(AttributeSet attributes, IndexReaderConfiguration readerConfiguration, IndexWriterConfiguration writerConfiguration, IndexShardingConfiguration shardingConfiguration) {
      super(Element.INDEXING, attributes);
      this.readerConfiguration = readerConfiguration;
      this.writerConfiguration = writerConfiguration;
      this.shardingConfiguration = shardingConfiguration;
   }

   /**
    * Determines if indexing is enabled for this cache configuration.
    */
   public boolean enabled() {
      return attributes.attribute(ENABLED).get();
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
    * Affects how cache operations will be propagated to the indexes.
    * By default, {@link IndexingMode#AUTO}.
    *
    * @see IndexingMode
    *
    * @return If the auto-indexing is enabled
    */
   public IndexingMode indexingMode() {
      return attributes.attribute(INDEXING_MODE).get();
   }

   /**
    * Whether true, the indexes will be defined on the Java indexed entities accessible from the class path of the server VM.
    * Even if the cache is Protobuf encoded and the indexes are run in server mode.
    * <p>
    * Useful in case we want to run embedded queries from a server task, in the case the cache is Protobuf encoded.
    * By default, false.
    *
    * @return useJavaEmbeddedEntities
    */
   public boolean useJavaEmbeddedEntities() {
      return attributes.attribute(USE_JAVA_EMBEDDED_ENTITIES).get();
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
    * The set of fully qualified names of indexed entity types, either Java classes or protobuf type names. This
    * configuration corresponds to the {@code <indexed-entities>} XML configuration element.
    */
   public Set<String> indexedEntityTypes() {
      return attributes.attribute(INDEXED_ENTITIES).get();
   }

   public IndexReaderConfiguration reader() {
      return readerConfiguration;
   }

   public IndexWriterConfiguration writer() {
      return writerConfiguration;
   }

   public IndexShardingConfiguration sharding() {
      return shardingConfiguration;
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
            ", writer=" + writerConfiguration +
            ", sharding=" + shardingConfiguration;
   }
}
