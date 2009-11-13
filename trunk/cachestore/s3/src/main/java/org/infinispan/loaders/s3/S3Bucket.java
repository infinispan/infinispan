package org.infinispan.loaders.s3;

import org.infinispan.loaders.bucket.Bucket;

import java.util.Set;

/**
 * <p/>
 * This interface defines the interactons between the {@link S3CacheStore} and Amazon S3.
 *
 * @author Adrian Cole
 * @since 4.0
 */
public interface S3Bucket<B, C extends S3Connection> {

   /**
    * Creates a connection to S3, and associates this object with an S3Bucket.
    *
    * @param connection - third-party connection to S3
    * @param bucket     - third-party representation of an  S3 Bucket
    */
   void init(C connection, B bucket);

   /**
    * @return name of the S3Bucket data will be stored in
    */
   String getName();

   /**
    * Adds the Infinispan bucket into the S3 Bucket at location {@link org.infinispan.loaders.bucket.Bucket#getBucketName()}
    *
    * @param object what to persist into S3
    */
   void insert(Bucket object) throws S3ConnectionException;

   /**
    * @param key - location in the S3Bucket where we can find the infinispan Bucket
    * @return Infinispan bucket associated with the key
    */
   Bucket get(String key) throws S3ConnectionException;

   /**
    * @return names all infinispan buckets stored in this S3 Bucket
    */
   Set<String> keySet() throws S3ConnectionException;

   /**
    * @return all infinispan buckets stored in this S3 Bucket
    */
   Set<Bucket> values() throws S3ConnectionException;

   /**
    * Removes the Infinispan bucket from the S3 Bucket at location <code>key</code>
    *
    * @param key what to remove from S3
    */
   public void remove(String key) throws S3ConnectionException;

   /**
    * removes all Infinispan buckets from the S3 bucket
    */
   void clear() throws S3ConnectionException;

}