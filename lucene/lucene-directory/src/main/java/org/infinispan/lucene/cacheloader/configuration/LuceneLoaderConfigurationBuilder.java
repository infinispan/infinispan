package org.infinispan.lucene.cacheloader.configuration;

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


   /** Auto-split huge files in blocks, with a base value of 32MB **/
   private int autoChunkSize = Integer.MAX_VALUE / 64;

   /** Path of the root directory containing all indexes **/
   private String location = "Infinispan-IndexStore";

   public LuceneLoaderConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder);
   }


   /**
    * When segment files are larger than this amount of bytes, the segment will be split into multiple chunks
    * of this size.
    *
    * @param autoChunkSize
    * @return this for method chaining
    */
   public LuceneLoaderConfigurationBuilder autoChunkSize(int autoChunkSize) {
      this.autoChunkSize = autoChunkSize;
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
      this.location = location;
      return this;
   }

   @Override
   public void validate() {
      // No op.
   }

   @Override
   public LuceneLoaderConfiguration create() {
      return new LuceneLoaderConfiguration(purgeOnStartup, fetchPersistentState, ignoreModifications, async.create(),
                                          singleton().create(), preload, shared, properties, this.autoChunkSize, this.location);
   }

   @Override
   public Builder<?> read(LuceneLoaderConfiguration template) {
      super.read(template);

      this.autoChunkSize = template.autoChunkSize();
      this.location = template.location();

      return this;
   }

   @Override
   public LuceneLoaderConfigurationBuilder self() {
      return this;
   }

   @Override
   public String toString() {
      return "LuceneLoaderConfigurationBuilder{" + "autoChunkSize=" + autoChunkSize + ", " +
            "location=" + location + "}";
   }
}
