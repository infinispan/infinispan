package org.infinispan.lucene.cachestore;

import org.infinispan.config.ConfigurationBeanVisitor;
import org.infinispan.loaders.CacheLoaderConfig;

/**
 * Configuration for a {@link LuceneCacheLoader}.
 * 
 * @author Sanne Grinovero
 * @since 5.2
 */
public final class LuceneCacheLoaderConfig implements CacheLoaderConfig {

   public static final String LOCATION_OPTION = "location";

   public static final String AUTO_CHUNK_SIZE_OPTION = "autoChunkSize";

   /**
    * Auto split huge files in blocks, by default of 32MB
    */
   protected int autoChunkSize = Integer.MAX_VALUE / 64;

   /**
    * Path of the root directory containing all indexes
    */
   protected String location = "Infinispan-IndexStore";

   /**
    * Path to the root directory containing all indexes. Indexes are loaded from the immediate subdirectories
    * of specified path, and each such subdirectory name will be the index name that must match the name
    * parameter of a {@link Directory} constructor.
    * 
    * @param location path to the root directory of all indexes
    * @return this for method chaining
    */
   public LuceneCacheLoaderConfig location(String location) {
      this.location = location;
      return this;
   }

   /**
    * When segment files are larger than this amount of bytes, the segment will be splitted in multiple chunks
    * if this size.
    * 
    * @param autoChunkSize
    * @return this for method chaining
    */
   public LuceneCacheLoaderConfig autoChunkSize(int autoChunkSize) {
      this.autoChunkSize = autoChunkSize;
      return this;
   }

   @Override
   public void accept(ConfigurationBeanVisitor visitor) {
      visitor.visitCacheLoaderConfig(this);
   }

   @Override
   public String getCacheLoaderClassName() {
      return LuceneCacheLoader.class.getName();
   }

   @Override
   public void setCacheLoaderClassName(String s) {
      //ignored
   }

   @Override
   public ClassLoader getClassLoader() {
      //we'll only need classes from this same module
      return LuceneCacheLoaderConfig.class.getClassLoader();
   }

   @Override
   public CacheLoaderConfig clone() {
      LuceneCacheLoaderConfig copy = new LuceneCacheLoaderConfig();
      copy.autoChunkSize = autoChunkSize;
      copy.location = location;
      return copy;
   }

}
