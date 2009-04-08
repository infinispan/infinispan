package org.infinispan.loader.s3;

import org.apache.commons.io.IOUtils;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Adrian Cole
 * @version $Id$
 * @since 4.0
 */
public class MockS3Connection implements S3Connection {
   private Map<String, S3Bucket> nameToS3Bucket = new ConcurrentHashMap<String, S3Bucket>();
   private Map<String, Map<String, S3Object>> bucketToContents = new ConcurrentHashMap<String, Map<String, S3Object>>();

   public synchronized S3Bucket getOrCreateBucket(String bucketName) throws S3ServiceException {
      S3Bucket bucket = nameToS3Bucket.get(bucketName);
      if (bucket == null) {
         bucket = new S3Bucket(bucketName);
         nameToS3Bucket.put(bucketName, bucket);
         bucketToContents.put(bucketName, new ConcurrentHashMap<String, S3Object>());
      }
      return bucket;
   }

   public S3Object[] getAllObjectsInBucketWithoutTheirData(S3Bucket bucket) throws S3ServiceException {
      Map<String, S3Object> contents = bucketToContents.get(bucket.getName());
      return contents.values().toArray(new S3Object[]{});
   }

   public void copyObjectsFromOneBucketToAnother(String[] keys, String sourceBucketName, String destinationBucketName) throws S3ServiceException {
      Map<String, S3Object> source = bucketToContents.get(sourceBucketName);
      Map<String, S3Object> destination = bucketToContents.get(destinationBucketName);
      for (int i = 0; i < keys.length; i++) {
         destination.put(keys[i], source.get(keys[i]));
      }
   }

   public void removeBucketIfEmpty(S3Bucket bucket) throws S3ServiceException {
      nameToS3Bucket.remove(bucket.getName());
      bucketToContents.remove(bucket.getName());
   }

   public S3Object getObjectInBucket(String objectKey, S3Bucket bucket) throws S3ServiceException {
      Map<String, S3Object> contents = bucketToContents.get(bucket.getName());
      return contents.get(objectKey);
   }

   public S3Object putObjectIntoBucket(S3Object object, S3Bucket bucket) throws S3ServiceException {
      Map<String, S3Object> contents = bucketToContents.get(bucket.getName());
      contents.put(object.getKey(), object);
      return object;
   }

   public void connect(String awsAccessKey, String awsSecretKey) throws S3ServiceException {
      // ignore
   }

   public S3Object createObject(String key) {
      return new MockS3Object(key);
   }

   public void removeObjectFromBucket(String objectKey, S3Bucket bucket) throws S3ServiceException {
      Map<String, S3Object> contents = bucketToContents.get(bucket.getName());
      contents.remove(objectKey);
   }

   public void removeAllObjectsFromBucket(S3Bucket bucket) throws S3ServiceException {
      Map<String, S3Object> contents = bucketToContents.get(bucket.getName());
      contents.clear();
   }

   class MockS3Object extends S3Object {

      byte[] buff;

      public MockS3Object(String key) {
         super(key);
      }

      @Override
      public void setDataInputStream(InputStream inputStream) {
         try {
            buff = IOUtils.toByteArray(inputStream);
         } catch (IOException e) {
            throw new RuntimeException(e);
         }
      }

      @Override
      public InputStream getDataInputStream() throws S3ServiceException {
         return (buff != null) ? new ByteArrayInputStream(buff) : null;
      }
   }

}

