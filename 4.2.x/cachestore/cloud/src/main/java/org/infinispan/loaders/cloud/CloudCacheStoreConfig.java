package org.infinispan.loaders.cloud;

import org.infinispan.loaders.LockSupportCacheStoreConfig;

/**
 * The cache store config bean for the {@link org.infinispan.loaders.cloud.CloudCacheStore}. This
 * allows you to tune a number of characteristics of the
 * {@link org.infinispan.loaders.cloud.CloudCacheStore}.
 * <p/>
 * <ul>
 * <li><tt>identity</tt> - A String that identifies you to the cloud provider. For example. with
 * AWS, this is your ACCESS KEY.</li>
 * <li><tt>password</tt> - A String that is used to authenticate you with the cloud provider. For
 * example. with AWS, this is your SECRET KEY.</li>
 * <li><tt>bucketPrefix</tt> - A String that is prepended to generated buckets or containers on the
 * cloud store. Buckets or containers are named {bucketPrefix}-{cacheName}.</li>
 * <li><tt>proxyHost</tt> - The host name of a proxy to use. Optional, no proxy is used if this is
 * un-set.</li>
 * <li><tt>proxyPort</tt> - The port of a proxy to use. Optional, no proxy is used if this is
 * un-set.</li>
 * <li><tt>requestTimeout</tt> - A timeout to use when communicating with the cloud storage
 * provider, in milliseconds. Defaults to 10000.</li>
 * <li><tt>lazyPurgingOnly</tt> - If enabled, then expired entries are only purged on access,
 * lazily, rather than by using the periodic eviction thread. Defaults to <tt>false</tt>.</li>
 * <li><tt>cloudService</tt> - The cloud service to use. Supported values are <tt>s3</tt> (Amazon
 * AWS), <tt>cloudfiles</tt> (Rackspace Cloud), <tt>azureblob</tt> (Microsoft Azure), and
 * <tt>atmos</tt> (Atmos Online Storage Service).</li>
 * <li><tt>maxConnections</tt> - The maximum number of concurrent connections to make to the cloud
 * provider. Defaults to 10.</li>
 * <li><tt>secure</tt> - Whether to use secure (SSL) connections or not. Defaults to <tt>true</tt>.</li>
 * <li><tt>compress</tt> - Whether to compress stored data. Defaults to <tt>true</tt>.</li>
 * <li><tt>cloudServiceLocation</tt> - the data center to use. Note that this is specific to the
 * cloud provider in question. E.g., Amazon's S3 service supports storage buckets in several
 * different locations. Valid strings for S3, for example, are <a href="http://github.com/jclouds/jclouds/blob/master/aws/core/src/main/java/org/jclouds/aws/domain/Region.java"
 * >here</a>. Optional, and defaults to <tt>DEFAULT</tt>.</li>
 * </ul>
 * 
 * @author Manik Surtani
 * @since 4.0
 */
public class CloudCacheStoreConfig extends LockSupportCacheStoreConfig {
   private String identity;
   private String password;
   private String bucketPrefix;
   private String proxyHost;
   private String proxyPort;
   private long requestTimeout = 10000;
   private boolean lazyPurgingOnly = false;
   private String cloudService;
   private int maxConnections = 10000;
   private boolean secure = true;
   private boolean compress = true;

   private String cloudServiceLocation = "DEFAULT";
   private static final long serialVersionUID = -9011054600279256849L;

   public CloudCacheStoreConfig() {
      setCacheLoaderClassName(CloudCacheStore.class.getName());
   }

   public String getBucketPrefix() {
      return bucketPrefix;
   }

   public void setBucketPrefix(String bucketPrefix) {
      this.bucketPrefix = bucketPrefix;
   }

   public String getCloudService() {
      return cloudService;
   }

   public void setCloudService(String cloudService) {
      this.cloudService = cloudService;
   }

   public String getIdentity() {
      return identity;
   }

   public void setIdentity(String identity) {
      this.identity = identity;
   }

   public boolean isLazyPurgingOnly() {
      return lazyPurgingOnly;
   }

   public void setLazyPurgingOnly(boolean lazyPurgingOnly) {
      this.lazyPurgingOnly = lazyPurgingOnly;
   }

   public int getMaxConnections() {
      return maxConnections;
   }

   public void setMaxConnections(int maxConnections) {
      this.maxConnections = maxConnections;
   }

   public String getPassword() {
      return password;
   }

   public void setPassword(String password) {
      this.password = password;
   }

   public String getProxyHost() {
      return proxyHost;
   }

   public void setProxyHost(String proxyHost) {
      this.proxyHost = proxyHost;
   }

   public String getProxyPort() {
      return proxyPort;
   }

   public void setProxyPort(String proxyPort) {
      this.proxyPort = proxyPort;
   }

   public long getRequestTimeout() {
      return requestTimeout;
   }

   public void setRequestTimeout(long requestTimeout) {
      this.requestTimeout = requestTimeout;
   }

   public boolean isSecure() {
      return secure;
   }

   public void setSecure(boolean secure) {
      this.secure = secure;
   }

   public boolean isCompress() {
      return compress;
   }

   public void setCompress(boolean compress) {
      this.compress = compress;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o)
         return true;
      if (o == null || getClass() != o.getClass())
         return false;
      if (!super.equals(o))
         return false;

      CloudCacheStoreConfig that = (CloudCacheStoreConfig) o;

      if (lazyPurgingOnly != that.lazyPurgingOnly)
         return false;
      if (maxConnections != that.maxConnections)
         return false;
      if (requestTimeout != that.requestTimeout)
         return false;
      if (secure != that.secure)
         return false;
      if (compress != that.compress)
         return false;
      if (bucketPrefix != null ? !bucketPrefix.equals(that.bucketPrefix)
               : that.bucketPrefix != null)
         return false;
      if (cloudService != null ? !cloudService.equals(that.cloudService)
               : that.cloudService != null)
         return false;
      if (cloudServiceLocation != null ? !cloudServiceLocation.equals(that.cloudServiceLocation)
               : that.cloudServiceLocation != null)
         return false;
      if (identity != null ? !identity.equals(that.identity) : that.identity != null)
         return false;
      if (password != null ? !password.equals(that.password) : that.password != null)
         return false;
      if (proxyHost != null ? !proxyHost.equals(that.proxyHost) : that.proxyHost != null)
         return false;
      if (proxyPort != null ? !proxyPort.equals(that.proxyPort) : that.proxyPort != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (identity != null ? identity.hashCode() : 0);
      result = 31 * result + (password != null ? password.hashCode() : 0);
      result = 31 * result + (bucketPrefix != null ? bucketPrefix.hashCode() : 0);
      result = 31 * result + (proxyHost != null ? proxyHost.hashCode() : 0);
      result = 31 * result + (proxyPort != null ? proxyPort.hashCode() : 0);
      result = 31 * result + (int) (requestTimeout ^ (requestTimeout >>> 32));
      result = 31 * result + (lazyPurgingOnly ? 1 : 0);
      result = 31 * result + (cloudService != null ? cloudService.hashCode() : 0);
      result = 31 * result + maxConnections;
      result = 31 * result + (secure ? 1 : 0);
      result = 31 * result + (compress ? 1 : 0);
      result = 31 * result + (cloudServiceLocation != null ? cloudServiceLocation.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "CloudCacheStoreConfig{" + "bucketPrefix='" + bucketPrefix + '\'' + ", identity='"
               + identity + '\'' + ", password='" + password + '\'' + ", proxyHost='" + proxyHost
               + '\'' + ", proxyPort='" + proxyPort + '\'' + ", requestTimeout=" + requestTimeout
               + ", lazyPurgingOnly=" + lazyPurgingOnly + ", cloudService='" + cloudService + '\''
               + ", maxConnections=" + maxConnections + ", secure=" + secure + ", compress="
               + compress + ", cloudServiceLocation='" + cloudServiceLocation + '\'' + '}';
   }

   public String getCloudServiceLocation() {
      return cloudServiceLocation;
   }

   public void setCloudServiceLocation(String loc) {
      this.cloudServiceLocation = loc;
   }
}