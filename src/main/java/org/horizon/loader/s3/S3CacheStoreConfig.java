package org.horizon.loader.s3;

import org.horizon.loader.LockSupportCacheStoreConfig;

/**
 * Configures {@link org.horizon.loader.s3.S3CacheStore}.  This allows you to tune a number of characteristics of the
 * {@link S3CacheStore}.
 * <p/>
 * <ul> <li><tt>awsAccessKey</tt> - identifies you as the party responsible for s3 requests.  This is required and there
 * is no default.</li> <li><tt>awsSecretKey</tt> - used to authenticate you as the owner of <tt>awsAccessKey</tt>.  This
 * is required and there is no default.</li> <li><tt>bucket</tt> - the name of the s3 bucket used to store cache data.
 * This is required and there is no default.</li> </ul>
 *
 * @author Adrian Cole
 * @since 1.0
 */
public class S3CacheStoreConfig extends LockSupportCacheStoreConfig {
   private String awsAccessKey;
   private String awsSecretKey;
   private String bucket;

   public S3CacheStoreConfig() {
      setCacheLoaderClassName(S3CacheStore.class.getName());
   }

   public String getAwsAccessKey() {
      return awsAccessKey;
   }

   public void setAwsAccessKey(String awsAccessKey) {
      this.awsAccessKey = awsAccessKey;
   }

   public String getAwsSecretKey() {
      return awsSecretKey;
   }

   public void setAwsSecretKey(String awsSecretKey) {
      this.awsSecretKey = awsSecretKey;
   }

   public String getBucket() {
      return bucket;
   }

   public void setBucket(String bucket) {
      this.bucket = bucket;
   }


}
