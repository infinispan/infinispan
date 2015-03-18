package org.infinispan.hibernate.search.spi;

import java.util.Properties;

/**
 * Configuration constants for the Infinispan integration
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2012 Red Hat Inc.
 */
public class InfinispanIntegration {

   private InfinispanIntegration() {
      //not meant to be instantiated
   }

   /**
    * Configuration attribute to override the value of {@link #DEFAULT_LOCKING_CACHENAME}.
    */
   public static final String LOCKING_CACHENAME = "locking_cachename";

   /**
    * Configuration attribute to override the value of {@link #DEFAULT_INDEXESDATA_CACHENAME}.
    */
   public static final String DATA_CACHENAME = "data_cachename";

   /**
    * Configuration attribute to override the value of {@link #DEFAULT_INDEXESMETADATA_CACHENAME}.
    */
   public static final String METADATA_CACHENAME = "metadata_cachename";

   /**
    * Default name of the cache used to store Locking metadata
    */
   public static final String DEFAULT_LOCKING_CACHENAME = "LuceneIndexesLocking";

   /**
    * Default name of the cache used to store Index Data
    */
   public static final String DEFAULT_INDEXESDATA_CACHENAME = "LuceneIndexesData";

   /**
    * Default name of the cache used to store Index MetaData
    */
   public static final String DEFAULT_INDEXESMETADATA_CACHENAME = "LuceneIndexesMetadata";

   /**
    * Configuration attribute to control if the writes to Index Metadata should be performed asynchronously.
    * <p/>
    * Defaults to {@code false} if the backend is configured as synchronous and defaults to {@code true} if the backend
    * is configured as asynchronous.
    * <p/>
    * Setting this to {@code true} might improve performance but is highly experimental.
    */
   public static final String WRITE_METADATA_ASYNC = "write_metadata_async";

   /**
    * @param properties the Hibernate Search configuration
    * @return the name of the Cache to be retrieved from the CacheManager to store Index Metadata
    */
   public static String getMetadataCacheName(Properties properties) {
      return properties.getProperty(METADATA_CACHENAME, DEFAULT_INDEXESMETADATA_CACHENAME);
   }

   /**
    * @param properties the Hibernate Search configuration
    * @return the name of the Cache to be retrieved from the CacheManager to store Index Data
    */
   public static String getDataCacheName(Properties properties) {
      return properties.getProperty(DATA_CACHENAME, DEFAULT_INDEXESDATA_CACHENAME);
   }

   /**
    * @param properties the Hibernate Search configuration
    * @return the name of the Cache to be retrieved from the CacheManager to store Locking metadata
    */
   public static String getLockingCacheName(Properties properties) {
      return properties.getProperty(LOCKING_CACHENAME, DEFAULT_LOCKING_CACHENAME);
   }

}
