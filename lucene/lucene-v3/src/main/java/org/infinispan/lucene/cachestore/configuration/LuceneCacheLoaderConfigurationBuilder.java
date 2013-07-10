package org.infinispan.lucene.cachestore.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.cache.AbstractLoaderConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;

/**
 * {@link org.infinispan.configuration.cache.ConfigurationBuilder} bean for the {@link LuceneCacheLoaderConfiguration}
 *
 * @author navssurtani
 * @since 6.0.0
 */
public class LuceneCacheLoaderConfigurationBuilder extends
      AbstractLoaderConfigurationBuilder<LuceneCacheLoaderConfiguration, LuceneCacheLoaderConfigurationBuilder> {


   /** Auto-split huge files in blocks, with a base value of 32MB **/
   private int autoChunkSize = Integer.MAX_VALUE/64;

   /** Path of the root directory containing all indexes **/
   private String location = "Infinispan-IndexStore";

   public LuceneCacheLoaderConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
   }


   /**
    * When segment files are larger than this amount of bytes, the segment will be split into multiple chunks
    * of this size.
    *
    * @param autoChunkSize
    * @return this for method chaining
    */
   public LuceneCacheLoaderConfigurationBuilder autoChunkSize(int autoChunkSize) {
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
   public LuceneCacheLoaderConfigurationBuilder location(String location) {
      this.location = location;
      return this;
   }

   @Override
   public void validate() {
      // No op.
   }

   @Override
   public LuceneCacheLoaderConfiguration create() {
      return new LuceneCacheLoaderConfiguration(this.autoChunkSize, this.location,
            TypedProperties.toTypedProperties(properties));
   }

   @Override
   public Builder<?> read(LuceneCacheLoaderConfiguration template) {
      this.autoChunkSize = template.autoChunkSize();
      this.location = template.location();
      return this;
   }

   @Override
   public LuceneCacheLoaderConfigurationBuilder self() {
      return this;
   }

   @Override
   public String toString() {
      return "LuceneCacheLoaderConfigurationBuilder{" + "autoChunkSize=" + autoChunkSize + ", " +
            "location=" + location + "}";
   }
}
