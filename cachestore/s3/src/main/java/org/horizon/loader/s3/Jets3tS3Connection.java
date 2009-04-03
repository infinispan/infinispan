package org.horizon.loader.s3;

import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.multithread.S3ServiceSimpleMulti;
import org.jets3t.service.security.AWSCredentials;

/**
 * A {@link org.jets3t.service.S3Service jets3t} implementation of {@link S3Connection}.
 * <p/>
 * Tuning and configuration parameters can be overridden by creating <tt>jets3t.properties</tt> and adding it to your
 * classpath.
 *
 * @author Adrian Cole
 * @link http://jets3t.s3.amazonaws.com/toolkit/configuration.html
 * @since 4.0
 */
public class Jets3tS3Connection implements S3Connection {
   private S3Service s3Service;
   private S3ServiceSimpleMulti s3MultiService;

   /**
    * {@inheritDoc}
    *
    * @see RestS3Service#RestS3Service(org.jets3t.service.security.AWSCredentials)
    * @see S3ServiceSimpleMulti#S3ServiceSimpleMulti(org.jets3t.service.S3Service)
    */
   public void connect(String awsAccessKey, String awsSecretKey) throws S3ServiceException {
      AWSCredentials awsCredentials =
            new AWSCredentials(awsAccessKey, awsSecretKey);
      s3Service = new RestS3Service(awsCredentials);
      s3MultiService = new S3ServiceSimpleMulti(s3Service);
   }

   /**
    * {@inheritDoc}
    *
    * @see S3Object#S3Object(String)
    */
   public S3Object createObject(String key) {
      return new S3Object(key);
   }

   /**
    * {@inheritDoc}
    *
    * @see org.jets3t.service.S3Service#deleteObject(org.jets3t.service.model.S3Bucket, String)
    */
   public void removeObjectFromBucket(String objectKey, S3Bucket bucket) throws S3ServiceException {
      s3Service.deleteObject(bucket, objectKey);
   }

   /**
    * {@inheritDoc}
    *
    * @see org.jets3t.service.S3Service#getBucket(String)
    * @see org.jets3t.service.S3Service#createBucket(String)
    */
   public S3Bucket getOrCreateBucket(String bucketName) throws S3ServiceException {
      return s3Service.getOrCreateBucket(bucketName);
   }

   /**
    * {@inheritDoc}
    *
    * @see org.jets3t.service.S3Service#listObjects(org.jets3t.service.model.S3Bucket)
    */
   public S3Object[] getAllObjectsInBucketWithoutTheirData(S3Bucket bucket) throws S3ServiceException {
      return s3Service.listObjects(bucket);
   }

   /**
    * {@inheritDoc}
    *
    * @see S3ServiceSimpleMulti#copyObjects(String, String, String[], org.jets3t.service.model.S3Object[], boolean)
    */
   public void copyObjectsFromOneBucketToAnother(String[] keys, String sourceBucketName, String destinationBucketName) throws S3ServiceException {
      S3Object[] destinationObjects = new S3Object[keys.length];
      int i = 0;
      for (String key : keys) {
         destinationObjects[i++] = createObject(key);
      }
      s3MultiService.copyObjects(sourceBucketName, destinationBucketName, keys, destinationObjects, false);
   }

   /**
    * {@inheritDoc}
    *
    * @see Jets3tS3Connection#getAllObjectsInBucketWithoutTheirData(org.jets3t.service.model.S3Bucket)
    * @see S3ServiceSimpleMulti#deleteObjects(org.jets3t.service.model.S3Bucket, org.jets3t.service.model.S3Object[])
    */
   public void removeAllObjectsFromBucket(S3Bucket bucket) throws S3ServiceException {
      S3Object[] objects = getAllObjectsInBucketWithoutTheirData(bucket);
      s3MultiService.deleteObjects(bucket, objects);
   }

   /**
    * {@inheritDoc}
    *
    * @see S3Service#deleteBucket(S3Bucket)
    */
   public void removeBucketIfEmpty(S3Bucket bucket) throws S3ServiceException {
      s3Service.deleteBucket(bucket);
   }

   /**
    * {@inheritDoc}
    *
    * @see S3Service#getObject(org.jets3t.service.model.S3Bucket, String)
    */
   public S3Object getObjectInBucket(String objectKey, S3Bucket bucket) throws S3ServiceException {
      try {
         return s3Service.getObject(bucket, objectKey);
      } catch (S3ServiceException e) {
         if (e.getS3ErrorCode() != null && e.getS3ErrorCode().equals("NoSuchKey")) {
            return null;
         }
         throw e;
      }
   }

   /**
    * {@inheritDoc}
    *
    * @see S3Service#putObject(org.jets3t.service.model.S3Bucket, org.jets3t.service.model.S3Object)
    */
   public S3Object putObjectIntoBucket(S3Object object, S3Bucket bucket) throws S3ServiceException {
      return s3Service.putObject(bucket, object);
   }
}