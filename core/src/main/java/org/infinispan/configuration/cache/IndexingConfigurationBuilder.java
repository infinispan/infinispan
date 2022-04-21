package org.infinispan.configuration.cache;

import static org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration.PROPERTIES;
import static org.infinispan.commons.util.StringPropertyReplacer.replaceProperties;
import static org.infinispan.configuration.cache.IndexingConfiguration.AUTO_CONFIG;
import static org.infinispan.configuration.cache.IndexingConfiguration.ENABLED;
import static org.infinispan.configuration.cache.IndexingConfiguration.INDEX;
import static org.infinispan.configuration.cache.IndexingConfiguration.INDEXED_ENTITIES;
import static org.infinispan.configuration.cache.IndexingConfiguration.KEY_TRANSFORMERS;
import static org.infinispan.configuration.cache.IndexingConfiguration.PATH;
import static org.infinispan.configuration.cache.IndexingConfiguration.STARTUP_MODE;
import static org.infinispan.configuration.cache.IndexingConfiguration.STORAGE;
import static org.infinispan.util.logging.Log.CONFIG;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * Configures indexing of entries in the cache for searching.
 */
public class IndexingConfigurationBuilder extends AbstractConfigurationChildBuilder implements IndexingConfigurationChildBuilder, Builder<IndexingConfiguration> {

   private static final String BACKEND_PREFIX = "hibernate.search.backend.";

   private static final String DIRECTORY_PROVIDER_KEY = BACKEND_PREFIX + "directory.type";

   private static final String EXCLUSIVE_INDEX_USE = "hibernate.search.default.exclusive_index_use";

   private static final String INDEX_MANAGER = "hibernate.search.default.indexmanager";

   private static final String READER_STRATEGY = "hibernate.search.default.reader.strategy";

   private static final String FS_PROVIDER = "local-filesystem";

   /**
    * Legacy name "ram" was replaced by "local-heap" many years ago.
    *
    * @deprecated To be removed after migration to hibernate search 6, if the version no longer supports this legacy
    * name.
    */
   @Deprecated
   private static final String RAM_DIRECTORY_PROVIDER = "ram";

   private static final String LOCAL_HEAP_DIRECTORY_PROVIDER = "local-heap";

   private final AttributeSet attributes;

   private final Set<Class<?>> resolvedIndexedClasses = new HashSet<>();

   private final IndexReaderConfigurationBuilder readerConfigurationBuilder;
   private final IndexWriterConfigurationBuilder writerConfigurationBuilder;

   IndexingConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      attributes = IndexingConfiguration.attributeDefinitionSet();
      readerConfigurationBuilder = new IndexReaderConfigurationBuilder(this);
      writerConfigurationBuilder = new IndexWriterConfigurationBuilder(this);
   }

   public IndexingConfigurationBuilder enabled(boolean enabled) {
      if (attributes.attribute(INDEX).isModified()) {
         throw CONFIG.indexEnabledAndIndexModeAreExclusive();
      }
      if (!enabled) {
         // discard any eventually inherited indexing config if indexing is not going to be enabled
         reset();
      }
      attributes.attribute(ENABLED).set(enabled);
      return this;
   }

   public IndexReaderConfigurationBuilder reader() {
      return readerConfigurationBuilder;
   }

   public IndexWriterConfigurationBuilder writer() {
      return writerConfigurationBuilder;
   }

   /**
    * Wipe out all indexing configuration settings and disable indexing.
    */
   public void reset() {
      attributes.attribute(INDEX).reset();
      attributes.attribute(AUTO_CONFIG).reset();
      attributes.attribute(ENABLED).reset();
      attributes.attribute(INDEXED_ENTITIES).reset();
      attributes.attribute(PROPERTIES).reset();
      attributes.attribute(KEY_TRANSFORMERS).reset();
      attributes.attribute(STORAGE).reset();
      attributes.attribute(STARTUP_MODE).reset();
      attributes.attribute(PATH).reset();
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

   /**
    * Registers a transformer for a key class.
    *
    * @param keyClass the class of the key
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
    * @deprecated Since 12.0, please use {@link #writer()} and {@link #reader()} to define indexing behavior.
    */
   @Deprecated
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
    * @deprecated Since 12.0, please {@link #writer()} and {@link #reader()} to define indexing behavior.
    */
   @Deprecated
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
    * @deprecated Since 12.0, please {@link #writer()} and {@link #reader()} to define indexing behavior.
    */
   @Deprecated
   public IndexingConfigurationBuilder withProperties(Properties props) {
      attributes.attribute(PROPERTIES).set(TypedProperties.toTypedProperties(props));
      return this;
   }

   /**
    * Indicates indexing mode
    *
    * @deprecated Since 11.0. This configuration will be removed in next major version as the index mode is calculated
    * automatically.
    */
   @Deprecated
   public IndexingConfigurationBuilder index(Index index) {
      if (attributes.attribute(ENABLED).isModified()) {
         throw CONFIG.indexEnabledAndIndexModeAreExclusive();
      }
      enabled(index != null && index != Index.NONE);
      attributes.attribute(INDEX).set(index);
      return this;
   }

   /**
    * When enabled, applies to properties default configurations based on
    * the cache type
    *
    * @param autoConfig boolean
    * @return <code>this</code>, for method chaining
    * @deprecated Since 11.0 with no replacement.
    */
   @Deprecated
   public IndexingConfigurationBuilder autoConfig(boolean autoConfig) {
      if (autoConfig && !attributes.attribute(ENABLED).isModified()) {
         enable();
      }
      attributes.attribute(AUTO_CONFIG).set(autoConfig);
      return this;
   }

   /**
    * @deprecated Since 11.0, with no replacement.
    */
   @Deprecated
   public boolean autoConfig() {
      return attributes.attribute(AUTO_CONFIG).get();
   }

   public IndexingConfigurationBuilder addIndexedEntity(String indexedEntity) {
      if (indexedEntity == null || indexedEntity.length() == 0) {
         throw new CacheConfigurationException("Type name must not be null or empty");
      }
      Set<String> indexedEntitySet = indexedEntities();
      indexedEntitySet.add(indexedEntity);
      attributes.attribute(INDEXED_ENTITIES).set(indexedEntitySet);
      return this;
   }

   public IndexingConfigurationBuilder addIndexedEntities(String... indexedEntities) {
      Set<String> indexedEntitySet = indexedEntities();
      for (String typeName : indexedEntities) {
         if (typeName == null || typeName.length() == 0) {
            throw new CacheConfigurationException("Type name must not be null or empty");
         }
         indexedEntitySet.add(typeName);
      }
      attributes.attribute(INDEXED_ENTITIES).set(indexedEntitySet);
      return this;
   }

   public IndexingConfigurationBuilder addIndexedEntity(Class<?> indexedEntity) {
      addIndexedEntity(indexedEntity.getName());
      resolvedIndexedClasses.add(indexedEntity);
      return this;
   }

   public IndexingConfigurationBuilder addIndexedEntities(Class<?>... indexedEntities) {
      addIndexedEntities(Arrays.stream(indexedEntities).map(Class::getName).toArray(String[]::new));
      Collections.addAll(resolvedIndexedClasses, indexedEntities);
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

      if (attributes.attribute(INDEX).get() == Index.PRIMARY_OWNER) {
         throw CONFIG.indexModeNotSupported(Index.PRIMARY_OWNER.name());
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
         applyLegacyProperties(attributes.attribute(PROPERTIES).get());
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

   private void applyAutoConfig(TypedProperties properties) {
      properties.putIfAbsent(DIRECTORY_PROVIDER_KEY, FS_PROVIDER);
      properties.putIfAbsent(EXCLUSIVE_INDEX_USE, "true");
      properties.putIfAbsent(INDEX_MANAGER, "near-real-time");
      properties.putIfAbsent(READER_STRATEGY, "shared");
   }

   @Override
   public IndexingConfiguration create() {
      TypedProperties typedProperties = attributes.attribute(PROPERTIES).get();
      if (typedProperties.size() > 0) {
         CONFIG.indexingPropertiesDeprecated(typedProperties);
         applyLegacyProperties(typedProperties);
      }
      if (autoConfig()) {
         applyAutoConfig(typedProperties);
         attributes.attribute(PROPERTIES).set(typedProperties);

         // check that after autoConfig we still do not have multiple configured providers
         ensureSingleIndexingConfig();
      }

      // todo [anistor] if storage media type is not configured then log a warning because this is not supported with indexing

      return new IndexingConfiguration(attributes.protect(), resolvedIndexedClasses, readerConfigurationBuilder.create(), writerConfigurationBuilder.create());
   }

   /**
    * Apply ISPN 11 index properties
    */
   private void applyLegacyProperties(TypedProperties properties) {
      properties.forEach((k, v) -> {
         String prop = k.toString();
         String propValue = v.toString();
         if (prop.endsWith(".directory_provider")) {
            if (LOCAL_HEAP_DIRECTORY_PROVIDER.equals(propValue)) {
               storage(IndexStorage.LOCAL_HEAP);
            } else {
               storage(IndexStorage.FILESYSTEM);
            }
         }
         if (prop.endsWith(".indexBase")) path(replaceProperties(propValue));
         if (prop.endsWith(".merge_factor")) writer().merge().factor(Integer.parseInt(propValue));
         if (prop.endsWith(".merge_max_size")) writer().merge().maxSize(Integer.parseInt(propValue));
         if (prop.endsWith(".ram_buffer_size")) writer().ramBufferSize(Integer.parseInt(propValue));
         if (prop.endsWith(".index_flush_interval")) writer().commitInterval(Integer.parseInt(propValue));
         if (prop.endsWith(".reader.async_refresh_period_ms")) reader().refreshInterval(Long.parseLong(propValue));
      });
   }

   @Override
   public IndexingConfigurationBuilder read(IndexingConfiguration template) {
      attributes.read(template.attributes());

      // ensures inheritance works properly even when inheriting from an old config
      // that uses INDEX or AUTO_CONFIG instead of ENABLED
      Index index = attributes.attribute(INDEX).get();
      if (index != null) {
         enabled(index != Index.NONE);
      }
      if (autoConfig() && !attributes.attribute(ENABLED).isModified()) {
         enable();
      }
      this.resolvedIndexedClasses.clear();
      this.resolvedIndexedClasses.addAll(template.indexedEntities());
      this.readerConfigurationBuilder.read(template.reader());
      this.writerConfigurationBuilder.read(template.writer());
      return this;
   }

   @Override
   public String toString() {
      return "IndexingConfigurationBuilder{" +
            "attributes=" + attributes +
            ", readerConfigurationBuilder=" + readerConfigurationBuilder +
            ", writerConfigurationBuilder=" + writerConfigurationBuilder +
            '}';
   }

}
