package org.infinispan.loaders.s3;

import org.infinispan.Cache;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.bucket.Bucket;
import org.infinispan.loaders.bucket.BucketBasedCacheStore;
import org.infinispan.loaders.s3.jclouds.JCloudsBucket;
import org.infinispan.loaders.s3.jclouds.JCloudsConnection;
import org.infinispan.marshall.Marshaller;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.Set;

/**
 * By default, a JClouds implementation of a {@link org.infinispan.loaders.bucket.BucketBasedCacheStore}. This file
 * store stores stuff in the following format: <tt>http://s3.amazon.com/{bucket}/bucket_number.bucket</tt>
 * <p/>
 *
 * @author Adrian Cole
 * @since 4.0
 */
public class S3CacheStore extends BucketBasedCacheStore {

   private static final Log log = LogFactory.getLog(S3CacheStore.class);

   private S3CacheStoreConfig config;

   private S3Connection connection;
   private S3Bucket s3Bucket;

   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      return S3CacheStoreConfig.class;
   }

   /**
    * {@inheritDoc} This initializes the internal <tt>s3Connection</tt> to a default implementation
    */
   public void init(CacheLoaderConfig cfg, Cache cache, Marshaller m) throws CacheLoaderException {
      this.config = (S3CacheStoreConfig) cfg;
      S3Bucket cloudsBucket;
      S3Connection cloudsConnection;
      try {
         cloudsConnection = config.getConnectionClass() != null ? (S3Connection) Util.getInstance(config.getConnectionClass()) : new JCloudsConnection();
         cloudsBucket = config.getBucketClass()!=null ? (S3Bucket) Util.getInstance(config.getBucketClass()) : new JCloudsBucket();
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      }
      init(cfg, cache, m, cloudsConnection, cloudsBucket);
   }

   @Override
   public void stop() throws CacheLoaderException {
      super.stop();
      this.connection.disconnect();
   }

   public void init(CacheLoaderConfig config, Cache cache, Marshaller m, S3Connection connection, S3Bucket bucket) throws CacheLoaderException {
      super.init(config, cache, m);
      this.config = (S3CacheStoreConfig) config;
      this.cache = cache;
      this.marshaller = m;
      this.connection = connection;
      this.s3Bucket = bucket;
   }


   @SuppressWarnings("unchecked")
   @Override
   public void start() throws CacheLoaderException {
      super.start();

      if (config.getAwsAccessKey() == null)
         throw new IllegalArgumentException("awsAccessKey must be set");
      if (config.getAwsSecretKey() == null)
         throw new IllegalArgumentException("awsSecretKey must be set");
      this.connection.connect(config, marshaller);
      if (config.getBucketPrefix() == null)
         throw new IllegalArgumentException("s3Bucket must be set");
      String s3Bucket = getThisBucketName();
      this.s3Bucket.init(this.connection, connection.verifyOrCreateBucket(s3Bucket));
   }

   private String getThisBucketName() {
      if (log.isTraceEnabled()) {
         log.trace("Bucket prefix is " + config.getBucketPrefix()  + " and cache name is " + cache.getName());
      }
      return config.getBucketPrefix() + "-" + cache.getName().toLowerCase();
   }

   @SuppressWarnings("unchecked")
   protected Set<InternalCacheEntry> loadAllLockSafe() throws CacheLoaderException {
      Set<InternalCacheEntry> result = new HashSet<InternalCacheEntry>();
      // TODO I don't know why this returns objects at the moment
      for (Bucket bucket : (Set<Bucket>) s3Bucket.values()) {
         if (bucket.removeExpiredEntries()) {
            saveBucket(bucket);
         }
         result.addAll(bucket.getStoredEntries());
      }
      return result;
   }

   protected void fromStreamLockSafe(ObjectInput objectInput) throws CacheLoaderException {
      String source;
      try {
         source = (String) objectInput.readObject();

      } catch (Exception e) {
         throw convertToCacheLoaderException("Error while reading from stream", e);
      }
      if (getThisBucketName().equals(source)) {
         log.info("Attempt to load the same s3 bucket ignored");
      } else {
         connection.copyBucket(source, getThisBucketName());
      }
   }

   protected void toStreamLockSafe(ObjectOutput objectOutput) throws CacheLoaderException {
      try {
         objectOutput.writeObject(getThisBucketName());
      } catch (IOException e) {
         throw convertToCacheLoaderException("Error while writing to stream", e);
      }
   }

   protected void clearLockSafe() throws CacheLoaderException {
      s3Bucket.clear();
   }

   CacheLoaderException convertToCacheLoaderException(String message, Exception caught) {
      return (caught instanceof CacheLoaderException) ? (CacheLoaderException) caught :
            new CacheLoaderException(message, caught);
   }

   protected void purgeInternal() throws CacheLoaderException {
      loadAll();
   }

   protected Bucket loadBucket(String bucketName) throws CacheLoaderException {
      return s3Bucket.get(bucketName);
   }


   protected void insertBucket(Bucket bucket) throws CacheLoaderException {
      s3Bucket.insert(bucket);
   }

   protected void saveBucket(Bucket bucket) throws CacheLoaderException {
      s3Bucket.insert(bucket);
   }
}
