package org.horizon.loader.s3;

import org.horizon.Cache;
import org.horizon.loader.CacheLoaderConfig;
import org.horizon.loader.CacheLoaderException;
import org.horizon.loader.StoredEntry;
import org.horizon.loader.bucket.Bucket;
import org.horizon.loader.bucket.BucketBasedCacheStore;
import org.horizon.loader.file.FileCacheStore;
import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;
import org.horizon.marshall.Marshaller;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.utils.ServiceUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.Set;

/**
 * A {@link org.jets3t.service.S3Service jets3t} implementation of a {@link org.horizon.loader.bucket.BucketBasedCacheStore}.
 * This file store stores stuff in the following format: <tt>http://s3.amazon.com/{bucket}/bucket_number.bucket</tt>
 * <p/>
 * Tuning and configuration parameters can be overridden by creating <tt>jets3t.properties</tt> and adding it to your
 * classpath.
 *
 * @author Adrian Cole
 * @link http://jets3t.s3.amazonaws.com/toolkit/configuration.html
 * @since 4.0
 */
public class S3CacheStore extends BucketBasedCacheStore {

   private static final Log log = LogFactory.getLog(FileCacheStore.class);

   private S3CacheStoreConfig config;
   private S3Bucket rootS3Bucket;

   Cache cache;
   Marshaller marshaller;

   private S3Connection s3Connection;

   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      return S3CacheStoreConfig.class;
   }

   /**
    * {@inheritDoc} This initializes the internal <tt>s3Connection</tt> as an implementation of {@link
    * Jets3tS3Connection}
    */
   public void init(CacheLoaderConfig config, Cache cache, Marshaller m) {
      init(config, cache, m, new Jets3tS3Connection());
   }

   public void init(CacheLoaderConfig config, Cache cache, Marshaller m, S3Connection s3Connection) {
      super.init(config, cache, m);
      this.config = (S3CacheStoreConfig) config;
      this.cache = cache;
      this.marshaller = m;
      this.s3Connection = s3Connection;
   }


   public void start() throws CacheLoaderException {
      super.start();

      String awsAccessKey = config.getAwsAccessKey();
      if (awsAccessKey == null)
         throw new IllegalArgumentException("awsAccessKey must be set");
      String awsSecretKey = config.getAwsSecretKey();
      if (awsSecretKey == null)
         throw new IllegalArgumentException("awsSecretKey must be set");
      String s3Bucket = config.getBucket();
      if (s3Bucket == null)
         throw new IllegalArgumentException("s3Bucket must be set");

      try {
         s3Connection.connect(awsAccessKey, awsSecretKey);
         rootS3Bucket = s3Connection.getOrCreateBucket(s3Bucket);
      } catch (S3ServiceException e) {
         throw convertToCacheLoaderException("error opening s3 service", e);
      }
   }

   protected Set<StoredEntry> loadAllLockSafe() throws CacheLoaderException {
      Set<StoredEntry> result = new HashSet<StoredEntry>();
      try {
         for (S3Object s3Object : s3Connection.getAllObjectsInBucketWithoutTheirData(rootS3Bucket)) {
            Bucket bucket = loadBucket(s3Object);
            if (bucket != null) {
               if (bucket.removeExpiredEntries()) {
                  saveBucket(bucket);
               }
               result.addAll(bucket.getStoredEntries());
            }
         }
      } catch (S3ServiceException e) {
         throw convertToCacheLoaderException("Error while loading entries", e);
      }
      return result;
   }

   protected void fromStreamLockSafe(ObjectInput objectInput) throws CacheLoaderException {
      try {
         S3Bucket source = (S3Bucket) objectInput.readObject();
         if (rootS3Bucket.getName().equals(source.getName())) {
            log.info("Attempt to load the same s3 bucket ignored");
         } else {
            S3Object[] sourceObjects = s3Connection.getAllObjectsInBucketWithoutTheirData(source);
            String[] sourceKeys = new String[sourceObjects.length];

            int i = 0;
            for (S3Object sourceObject : sourceObjects) {
               sourceKeys[i++] = sourceObject.getKey();
            }
            s3Connection.copyObjectsFromOneBucketToAnother(sourceKeys, source.getName(), rootS3Bucket.getName());
         }
         loadAll();
      } catch (Exception e) {
         throw convertToCacheLoaderException("Error while reading from stream", e);
      }
   }

   protected void toStreamLockSafe(ObjectOutput objectOutput) throws CacheLoaderException {
      try {
         objectOutput.writeObject(rootS3Bucket);
      } catch (IOException e) {
         throw convertToCacheLoaderException("Error while writing to stream", e);
      }
   }

   protected void clearLockSafe() throws CacheLoaderException {
      try {
         s3Connection.removeAllObjectsFromBucket(rootS3Bucket);
      } catch (S3ServiceException caught) {
         throw convertToCacheLoaderException("error recreating bucket " + config.getBucket(), caught);
      }
   }

   CacheLoaderException convertToCacheLoaderException(String message, Exception caught) {
      return (caught instanceof CacheLoaderException) ? (CacheLoaderException) caught :
            new CacheLoaderException(message, caught);
   }

   protected void purgeInternal() throws CacheLoaderException {
      loadAll();
   }

   protected Bucket loadBucket(String bucketName) throws CacheLoaderException {
      return loadBucket(s3Connection.createObject(bucketName));
   }

   protected Bucket loadBucket(S3Object s3Object) throws CacheLoaderException {
      Bucket bucket = null;
      InputStream is = null;
      ObjectInputStream ois = null;
      String key = s3Object.getKey();
      try {
         // it is possible that the S3Object above only holds details.  Try to fetch, if this is the case
         if (s3Object.getDataInputStream() == null) {
            s3Object = s3Connection.getObjectInBucket(key, rootS3Bucket);
         }

         // it is possible that the object never existed. in this case, fall out.
         if (s3Object != null && s3Object.getDataInputStream() != null) {
            is = s3Object.getDataInputStream();
            ois = new ObjectInputStream(is);
            bucket = (Bucket) ois.readObject();
            s3Object.closeDataInputStream();
            bucket.setBucketName(s3Object.getKey());
         }
      } catch (Exception e) {
         throw convertToCacheLoaderException("Error while reading from object: " + key, e);
      } finally {
         safeClose(ois);
         safeClose(is);
      }
      return bucket;
   }

   protected void insertBucket(Bucket bucket) throws CacheLoaderException {
      saveBucket(bucket);
   }

   public final void saveBucket(Bucket b) throws CacheLoaderException {
      try {
         if (b.getEntries().isEmpty()) {
            s3Connection.removeObjectFromBucket(b.getBucketName(), rootS3Bucket);
         } else {
            ByteArrayInputStream dataIS = new ByteArrayInputStream(
                  marshaller.objectToByteBuffer(b));
            byte[] md5Hash = ServiceUtils.computeMD5Hash(dataIS);
            dataIS.reset();
            S3Object s3Object = s3Connection.createObject(b.getBucketName());
            s3Object.setDataInputStream(dataIS);
            s3Object.setContentLength(dataIS.available());
            s3Object.setMd5Hash(md5Hash);
            s3Connection.putObjectIntoBucket(s3Object, rootS3Bucket);
         }
      } catch (Exception ex) {
         throw convertToCacheLoaderException("Exception while saving bucket " + b, ex);
      }
   }

}
