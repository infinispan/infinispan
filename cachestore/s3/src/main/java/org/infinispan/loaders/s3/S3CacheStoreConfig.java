package org.infinispan.loaders.s3;

import org.infinispan.loaders.LockSupportCacheStoreConfig;

/**
 * Configures {@link org.infinispan.loaders.s3.S3CacheStore}.  This allows you to tune a number of characteristics of
 * the {@link S3CacheStore}.
 * <p/>
 * <ul> <li><tt>awsAccessKey</tt> - identifies you as the party responsible for s3 requests.  This is required and there
 * is no default.</li> <li><tt>awsSecretKey</tt> - used to authenticate you as the owner of <tt>awsAccessKey</tt>.  This
 * is required and there is no default.</li> <li><tt>bucket</tt> - the name of the s3 bucket used to store cache data.
 * This is required and there is no default.</li> <li><tt>requestTimeout</tt> - The maximum amount of milliseconds a
 * single S3 request can take before throwing an exception.  Default is 10000</li><li><tt>lazyPurgingOnly</tt> - Causes
 * {@link org.infinispan.loaders.CacheStore#purgeExpired()} to be a no-op, and only removes expired entries lazily, on a
 * {@link org.infinispan.loaders.CacheLoader#load(Object)}.  Defaults to <tt>true</tt>.</li></ul>
 *
 * @author Adrian Cole
 * @since 4.0
 */
public class S3CacheStoreConfig extends LockSupportCacheStoreConfig {
   private String awsAccessKey;
   private String awsSecretKey;
   private String bucketPrefix;
   private String proxyHost;
   private int proxyPort;
   private long requestTimeout = 10000;
   private String bucketClass;
   private String connectionClass;
   private Boolean lazyPurgingOnly = true;


   public Boolean isLazyPurgingOnly() {
      return lazyPurgingOnly;
   }

   public void setLazyPurgingOnly(Boolean lazyPurgingOnly) {
      this.lazyPurgingOnly = lazyPurgingOnly;
   }

   public long getRequestTimeout() {
      return requestTimeout;
   }

   public void setRequestTimeout(long requestTimeout) {
      this.requestTimeout = requestTimeout;
   }


   public int getMaxConnections() {
      return maxConnections;
   }

   public void setMaxConnections(int maxConnections) {
      this.maxConnections = maxConnections;
   }

   private int maxConnections = 3;
   private boolean secure = true;

   public boolean isSecure() {
      return secure;
   }

   public void setSecure(boolean secure) {
      this.secure = secure;
   }


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

   public String getBucketPrefix() {
      return bucketPrefix;
   }

   public void setBucketPrefix(String bucketPrefix) {
      this.bucketPrefix = bucketPrefix;
   }

   public String getProxyHost() {
      return proxyHost;
   }

   public void setProxyHost(String proxyHost) {
      this.proxyHost = proxyHost;
   }

   public int getProxyPort() {
      return proxyPort;
   }

   public void setProxyPort(int proxyPort) {
      this.proxyPort = proxyPort;
   }

   public String getBucketClass() {
      return bucketClass;
   }

   public void setBucketClass(String bucketClass) {
      this.bucketClass = bucketClass;
   }

   public String getConnectionClass() {
      return connectionClass;
   }

   public void setConnectionClass(String connectionClass) {
      this.connectionClass = connectionClass;
   }
}