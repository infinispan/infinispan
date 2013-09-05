package org.infinispan.lucene.cachestore.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.lucene.cachestore.LuceneCacheLoader;

import java.util.Properties;

/**
 * Configuration bean for the {@link LuceneCacheLoader}.
 *
 * @author navssurtani
 * @since 6.0.0
 */
@BuiltBy(LuceneStoreConfigurationBuilder.class)
@ConfigurationFor(LuceneCacheLoader.class)
public class LuceneStoreConfiguration extends AbstractStoreConfiguration {

   private final int autoChunkSize;

   private final String location;


   public LuceneStoreConfiguration(boolean purgeOnStartup, boolean fetchPersistentState, boolean ignoreModifications,
                                   AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore, boolean preload,
                                   boolean shared, Properties properties, int autoChunkSize, String location) {
      super(purgeOnStartup, fetchPersistentState, ignoreModifications, async, singletonStore, preload, shared, properties);
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
