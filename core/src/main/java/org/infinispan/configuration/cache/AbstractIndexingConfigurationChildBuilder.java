package org.infinispan.configuration.cache;

/**
 * @since 12.0
 */
abstract class AbstractIndexingConfigurationChildBuilder
      extends AbstractConfigurationChildBuilder implements IndexingConfigurationChildBuilder {

   private final IndexingConfigurationBuilder indexingBuilder;

   protected AbstractIndexingConfigurationChildBuilder(IndexingConfigurationBuilder builder) {
      super(builder.getBuilder());
      this.indexingBuilder = builder;
   }

   @Override
   public IndexReaderConfigurationBuilder reader() {
      return indexingBuilder.reader();
   }

   @Override
   public IndexWriterConfigurationBuilder writer() {
      return indexingBuilder.writer();
   }

   @Override
   public IndexingConfigurationBuilder addIndexedEntity(String entity) {
      return indexingBuilder.addIndexedEntities(entity);
   }

   @Override
   public IndexingConfigurationBuilder enable() {
      return indexingBuilder.enable();
   }

   @Override
   public IndexingConfigurationBuilder disable() {
      return indexingBuilder.disable();
   }

   @Override
   public IndexingConfigurationBuilder path(String path) {
      return indexingBuilder.path(path);
   }

   @Override
   public IndexingConfigurationBuilder storage(IndexStorage storage) {
      return indexingBuilder.storage(storage);
   }

   @Override
   public IndexingConfigurationBuilder addKeyTransformer(Class<?> keyClass, Class<?> keyTransformerClass) {
      return indexingBuilder.addKeyTransformer(keyClass, keyTransformerClass);
   }

   @Override
   public IndexingConfigurationBuilder addIndexedEntities(String... indexedEntities) {
      return indexingBuilder.addIndexedEntities(indexedEntities);
   }

   @Override
   public IndexingConfigurationBuilder addIndexedEntity(Class<?> indexedEntity) {
      return indexingBuilder.addIndexedEntity(indexedEntity);
   }

   @Override
   public IndexingConfigurationBuilder addIndexedEntities(Class<?>... indexedEntities) {
      return indexingBuilder.addIndexedEntities(indexedEntities);
   }
}
