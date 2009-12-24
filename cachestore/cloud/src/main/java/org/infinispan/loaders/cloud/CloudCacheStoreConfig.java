package org.infinispan.loaders.cloud;

import org.infinispan.loaders.LockSupportCacheStoreConfig;

/**
 * Configures {@link CloudCacheStore}.  This allows you to tune a number of characteristics of
 * the {@link CloudCacheStore}.
 * <p/>
 * <ul>
 * <li><tt>cloudService</tt> - the cloud service provider to be used.  For supported values, see <a href="http://TODO">this page on JClouds' website</a> with supported provider strings.  This is required and there is no default.</li> 
 * <li><tt>identity</tt> - identifies you as the party responsible for cloud requests.  This is required and there
 * is no default.  This is dependent on your cloud provider backend.  For example, with Amazon Web Services, this is your AWS_ACCESS_KEY.</li>
 * <li><tt>password</tt> - used to authenticate you as the owner of <tt>identity</tt>.  This
 * is required and there is no default.  For example, with Amazon Web Services, this is your AWS_SECRET_KEY.</li>
 * <li><tt>bucket</tt> - the name of the cloud bucket used to store cache data.
 * This is required and there is no default.</li> <li><tt>requestTimeout</tt> - The maximum amount of milliseconds a
 * single request can take before throwing an exception.  Default is 10000</li><li><tt>lazyPurgingOnly</tt> - Causes
 * {@link org.infinispan.loaders.CacheStore#purgeExpired()} to be a no-op, and only removes expired entries lazily, on a
 * {@link org.infinispan.loaders.CacheLoader#load(Object)}.  Defaults to <tt>true</tt>.</li></ul>
 *
 * @author Adrian Cole
 * @author Manik Surtani
 * @since 4.0
 */
public class CloudCacheStoreConfig extends LockSupportCacheStoreConfig {
   private String identity;
   private String password;
   private String bucketPrefix;
   private String proxyHost;
   private int proxyPort;
   private long requestTimeout = 10000;
   private Boolean lazyPurgingOnly = true;
   private String cloudService;


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


   public CloudCacheStoreConfig() {
      setCacheLoaderClassName(CloudCacheStore.class.getName());
   }

   public String getIdentity() {
      return identity;
   }


   public void setIdentity(String identity) {
      this.identity = identity;
   }

   public String getPassword() {
      return password;
   }

   public void setPassword(String password) {
      this.password = password;
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

   public String getCloudService() {
      return cloudService;
   }

   public void setCloudService(String cloudService) {
      this.cloudService = cloudService;
   }
}