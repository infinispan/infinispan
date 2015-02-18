package org.infinispan.lucene.cacheloader.configuration;

import static org.infinispan.lucene.cacheloader.configuration.LuceneLoaderConfiguration.AUTO_CHUNK_SIZE;
import static org.infinispan.lucene.cacheloader.configuration.LuceneLoaderConfiguration.LOCATION;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
/**
 * {@link org.infinispan.configuration.cache.ConfigurationBuilder} bean for the {@link LuceneLoaderConfiguration}
 *
 * @author navssurtani
 * @since 6.0.0
 */
public class LuceneLoaderConfigurationBuilder extends
                                             AbstractStoreConfigurationBuilder<LuceneLoaderConfiguration, LuceneLoaderConfigurationBuilder> {

   public LuceneLoaderConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder, LuceneLoaderConfiguration.attributeDefinitionSet());
   }


   /**
    * When segment files are larger than this amount of bytes, the segment will be split into multiple chunks
    * of this size.
    *
    * @param autoChunkSize
    * @return this for method chaining
    */
   public LuceneLoaderConfigurationBuilder autoChunkSize(int autoChunkSize) {
      attributes.attribute(AUTO_CHUNK_SIZE).set(autoChunkSize);
      return this;
   }

   /**
    * Path to the root directory containing all indexes. Indexes are loaded from the immediate subdirectories
    * of specified path, and each such subdirectory name will be the index name that must match the name
    * parameter of a Directory constructor.
    *
    * @param location path to the root directory of all indexes
    * @return this for method chaining
    */
   public LuceneLoaderConfigurationBuilder location(String location) {
      attributes.attribute(LOCATION).set(location);
      return this;
   }

   @Override
   public void validate() {
      // No op.
   }

   @Override
   public LuceneLoaderConfiguration create() {
      return new LuceneLoaderConfiguration(attributes.protect(), async.create(), singletonStore.create());
   }

   @Override
   public Builder<?> read(LuceneLoaderConfiguration template) {
      super.read(template);
      return this;
   }

   @Override
   public LuceneLoaderConfigurationBuilder self() {
      return this;
   }

   @Override
   public String toString() {
      return "LuceneLoaderConfigurationBuilder [attributes=" + attributes + ", async=" + async + ", singletonStore=" + singletonStore + "]";
   }
}
