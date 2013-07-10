package org.infinispan.lucene.cachestore.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.cache.AbstractLoaderConfiguration;
import org.infinispan.lucene.cachestore.LuceneCacheLoader;

/**
 * Configuration bean for the {@link LuceneCacheLoader}.
 *
 * @author navssurtani
 * @since 6.0.0
 */
@BuiltBy(LuceneCacheLoaderConfigurationBuilder.class)
@ConfigurationFor(LuceneCacheLoader.class)
public class LuceneCacheLoaderConfiguration extends AbstractLoaderConfiguration {

   private final int autoChunkSize;

   private final String location;

   /**
    * Public constructor called by {@link LuceneCacheLoaderConfigurationBuilder}
    *
    * @param autoChunkSize
    * @param location
    */
   public LuceneCacheLoaderConfiguration(int autoChunkSize, String location, TypedProperties properties) {
      super(properties);
      this.autoChunkSize = autoChunkSize;
      this.location = location;
   }

   /**
    * When the segment size is larger than this number of bytes, separate segments will be created of this particular
    * size.
    *
    * @return the segmentation size.
    */
   public int autoChunkSize() {
      return this.autoChunkSize;
   }

   /**
    * The location of the root directory of the index.
    *
    * @return the index location root directory.
    */
   public String location() {
      return this.location;
   }
}
