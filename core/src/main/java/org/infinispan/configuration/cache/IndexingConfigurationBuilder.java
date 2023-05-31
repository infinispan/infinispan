package org.infinispan.configuration.cache;

import static org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration.PROPERTIES;
import static org.infinispan.commons.util.StringPropertyReplacer.replaceProperties;
import static org.infinispan.configuration.cache.IndexingConfiguration.ENABLED;
import static org.infinispan.configuration.cache.IndexingConfiguration.INDEXED_ENTITIES;
import static org.infinispan.configuration.cache.IndexingConfiguration.INDEXING_MODE;
import static org.infinispan.configuration.cache.IndexingConfiguration.KEY_TRANSFORMERS;
import static org.infinispan.configuration.cache.IndexingConfiguration.PATH;
import static org.infinispan.configuration.cache.IndexingConfiguration.STARTUP_MODE;
import static org.infinispan.configuration.cache.IndexingConfiguration.STORAGE;
import static org.infinispan.util.logging.Log.CONFIG;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.util.logging.Log;

/**
 * Configures indexing of entries in the cache for searching.
 */
public class IndexingConfigurationBuilder extends AbstractConfigurationChildBuilder implements IndexingConfigurationChildBuilder, Builder<IndexingConfiguration> {

   private final AttributeSet attributes;
   private final IndexReaderConfigurationBuilder readerConfigurationBuilder;
   private final IndexWriterConfigurationBuilder writerConfigurationBuilder;
   private final IndexShardingConfigurationBuilder shardingConfigurationBuilder;

   IndexingConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      attributes = IndexingConfiguration.attributeDefinitionSet();
      readerConfigurationBuilder = new IndexReaderConfigurationBuilder(this);
      writerConfigurationBuilder = new IndexWriterConfigurationBuilder(this);
      shardingConfigurationBuilder = new IndexShardingConfigurationBuilder(this);
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public IndexingConfigurationBuilder enabled(boolean enabled) {
      attributes.attribute(ENABLED).set(enabled);
      return this;
   }

   public IndexReaderConfigurationBuilder reader() {
      return readerConfigurationBuilder;
   }

   public IndexWriterConfigurationBuilder writer() {
      return writerConfigurationBuilder;
   }

   public IndexShardingConfigurationBuilder sharding() {
      return shardingConfigurationBuilder;
   }

   public IndexingConfigurationBuilder enable() {
      return enabled(true);
   }

   public IndexingConfigurationBuilder disable() {
      return enabled(false);
   }

   public boolean enabled() {
      return attributes.attribute(ENABLED).get();
   }

   public IndexingConfigurationBuilder path(String path) {
      attributes.attribute(PATH).set(path);
      return this;
   }

   public IndexingConfigurationBuilder storage(IndexStorage storage) {
      attributes.attribute(STORAGE).set(storage);
      return this;
   }

   public IndexingConfigurationBuilder startupMode(IndexStartupMode startupMode) {
      attributes.attribute(STARTUP_MODE).set(startupMode);
      return this;
   }

   public IndexingConfigurationBuilder indexingMode(IndexingMode indexingMode) {
      attributes.attribute(INDEXING_MODE).set(indexingMode);
      return this;
   }

   /**
    * Registers a transformer for a key class.
    *
    * @param keyClass            the class of the key
    * @param keyTransformerClass the class of the org.infinispan.query.Transformer that handles this key type
    * @return <code>this</code>, for method chaining
    */
   public IndexingConfigurationBuilder addKeyTransformer(Class<?> keyClass, Class<?> keyTransformerClass) {
      Map<Class<?>, Class<?>> keyTransformers = keyTransformers();
      keyTransformers.put(keyClass, keyTransformerClass);
      attributes.attribute(KEY_TRANSFORMERS).set(keyTransformers);
      return this;
   }

   /**
    * The currently configured key transformers.
    *
    * @return a {@link Map} in which the map key is the key class and the value is the Transformer class.
    */
   private Map<Class<?>, Class<?>> keyTransformers() {
      return attributes.attribute(KEY_TRANSFORMERS).get();
   }

   public IndexingConfigurationBuilder addIndexedEntity(String indexedEntity) {
      if (indexedEntity == null || indexedEntity.isEmpty()) {
         throw Log.CONFIG.indexedEntityNameMissing();
      }
      Set<String> indexedEntitySet = indexedEntities();
      indexedEntitySet.add(indexedEntity);
      attributes.attribute(INDEXED_ENTITIES).set(indexedEntitySet);
      return this;
   }

   public IndexingConfigurationBuilder addIndexedEntities(String... indexedEntities) {
      Set<String> indexedEntitySet = indexedEntities();
      for (String typeName : indexedEntities) {
         if (typeName == null || typeName.isEmpty()) {
            throw Log.CONFIG.indexedEntityNameMissing();
         }
         indexedEntitySet.add(typeName);
      }
      attributes.attribute(INDEXED_ENTITIES).set(indexedEntitySet);
      return this;
   }

   public IndexingConfigurationBuilder addIndexedEntity(Class<?> indexedEntity) {
      addIndexedEntity(indexedEntity.getName());
      return this;
   }

   public IndexingConfigurationBuilder addIndexedEntities(Class<?>... indexedEntities) {
      addIndexedEntities(Arrays.stream(indexedEntities).map(Class::getName).toArray(String[]::new));
      return this;
   }

   /**
    * The set of fully qualified names of indexed entity types, either Java classes or protobuf type names. This
    * configuration corresponds to the {@code <indexed-entities>} XML configuration element.
    */
   private Set<String> indexedEntities() {
      return attributes.attribute(INDEXED_ENTITIES).get();
   }

   @Override
   public void validate() {
      if (enabled()) {
         //Indexing is not conceptually compatible with Invalidation mode
         if (clustering().cacheMode().isInvalidation()) {
            throw CONFIG.invalidConfigurationIndexingWithInvalidation();
         }
         if (indexedEntities().isEmpty() && !getBuilder().template()) {
            //TODO  [anistor] This does not take into account eventual programmatically defined entity mappings
            throw CONFIG.noIndexableClassesDefined();
         }
      } else {
         if (!indexedEntities().isEmpty()) {
            throw CONFIG.indexableClassesDefined();
         }
      }

      ensureSingleIndexingConfig();
   }

   private void ensureSingleIndexingConfig() {
      TypedProperties typedProperties = attributes.attribute(PROPERTIES).get();
      boolean hasMultiIndexConfig =
            typedProperties.keySet().stream()
                  .map(Object::toString)
                  .filter(k -> k.contains("."))
                  .map(k -> k.substring(k.lastIndexOf('.')))
                  .anyMatch(s -> typedProperties.keySet().stream().filter(k -> k.toString().endsWith(s)).count() > 1);
      if (hasMultiIndexConfig) {
         throw CONFIG.foundDifferentIndexConfigPerType();
      }
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
      if (enabled()) {
         // Check that the query module is on the classpath.
         try {
            String clazz = "org.infinispan.query.Search";
            Util.loadClassStrict(clazz, globalConfig.classLoader());
         } catch (ClassNotFoundException e) {
            throw CONFIG.invalidConfigurationIndexingWithoutModule();
         }
         if (attributes.attribute(STORAGE).get() == IndexStorage.FILESYSTEM) {
            boolean globalStateEnabled = globalConfig.globalState().enabled();
            String path = replaceProperties(attributes.attribute(PATH).get());
            if (!globalStateEnabled) {
               if (path == null) {
                  CONFIG.indexLocationWorkingDir();
               } else if (!Paths.get(path).isAbsolute()) {
                  CONFIG.indexRelativeWorkingDir(path);
               }
            }
         }
      }
   }

   @Override
   public IndexingConfiguration create() {
      return new IndexingConfiguration(attributes.protect(), readerConfigurationBuilder.create(),
            writerConfigurationBuilder.create(), shardingConfigurationBuilder.create());
   }

   @Override
   public IndexingConfigurationBuilder read(IndexingConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      this.readerConfigurationBuilder.read(template.reader(), combine);
      this.writerConfigurationBuilder.read(template.writer(), combine);
      this.shardingConfigurationBuilder.read(template.sharding(), combine);
      return this;
   }

   @Override
   public String toString() {
      return "IndexingConfigurationBuilder{" +
            "attributes=" + attributes +
            ", readerConfigurationBuilder=" + readerConfigurationBuilder +
            ", writerConfigurationBuilder=" + writerConfigurationBuilder +
            ", shardingConfigurationBuilder=" + shardingConfigurationBuilder +
            '}';
   }

}
