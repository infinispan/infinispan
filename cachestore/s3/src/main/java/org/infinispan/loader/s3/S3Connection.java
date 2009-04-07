package org.infinispan.loader.s3;

import org.jets3t.service.S3ServiceException;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;

/**
 * This interface defines the interactons between the {@link S3CacheStore} and Amazon S3.
 *
 * @author Adrian Cole
 * @since 4.0
 */
public interface S3Connection {

   void connect(String awsAccessKey, String awsSecretKey) throws S3ServiceException;

   S3Bucket getOrCreateBucket(String bucketName) throws S3ServiceException;

   void removeBucketIfEmpty(S3Bucket bucket) throws S3ServiceException;

   S3Object createObject(String key);

   S3Object putObjectIntoBucket(S3Object object, S3Bucket bucket) throws S3ServiceException;

   S3Object getObjectInBucket(String objectKey, S3Bucket bucket) throws S3ServiceException;

   S3Object[] getAllObjectsInBucketWithoutTheirData(S3Bucket bucket) throws S3ServiceException;

   public void removeObjectFromBucket(String objectKey, S3Bucket bucket) throws S3ServiceException;

   void removeAllObjectsFromBucket(S3Bucket rootS3Bucket) throws S3ServiceException;

   void copyObjectsFromOneBucketToAnother(String[] keys, String sourceBucketName, String destinationBucketName) throws S3ServiceException;

}