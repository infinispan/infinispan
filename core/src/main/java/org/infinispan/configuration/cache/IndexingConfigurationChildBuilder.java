package org.infinispan.configuration.cache;

/**
 * @since 12.0
 */
public interface IndexingConfigurationChildBuilder extends ConfigurationChildBuilder {

   IndexReaderConfigurationBuilder reader();

   IndexWriterConfigurationBuilder writer();

   IndexingConfigurationBuilder addKeyTransformer(Class<?> keyClass, Class<?> keyTransformerClass);

   IndexingConfigurationBuilder addIndexedEntity(String indexedEntity);

   IndexingConfigurationBuilder addIndexedEntities(String... indexedEntities);

   IndexingConfigurationBuilder addIndexedEntity(Class<?> indexedEntity);

   IndexingConfigurationBuilder addIndexedEntities(Class<?>... indexedEntities);

   IndexingConfigurationBuilder disable();

   IndexingConfigurationBuilder enable();

   IndexingConfigurationBuilder path(String path);

   IndexingConfigurationBuilder storage(IndexStorage storage);

}
