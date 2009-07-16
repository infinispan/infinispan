package org.infinispan.loaders.s3;

import org.infinispan.config.ConfigurationElement;
import org.infinispan.config.ConfigurationElements;
import org.infinispan.config.ConfigurationProperty;
import org.infinispan.config.ConfigurationElement.Cardinality;
import org.infinispan.loaders.LockSupportCacheStoreConfig;

/**
 * Configures {@link org.infinispan.loaders.s3.S3CacheStore}.  This allows you to tune a number of characteristics of
 * the {@link S3CacheStore}.
 * <p/>
 * <ul> <li><tt>awsAccessKey</tt> - identifies you as the party responsible for s3 requests.  This is required and there
 * is no default.</li> <li><tt>awsSecretKey</tt> - used to authenticate you as the owner of <tt>awsAccessKey</tt>.  This
 * is required and there is no default.</li> <li><tt>bucket</tt> - the name of the s3 bucket used to store cache data.
 * This is required and there is no default.</li> <li><tt>requestTimeout</tt> - The maximum amount of milliseconds a
 * single S3 request can take before throwing an exception.  Default is 10000</li></ul>
 *
 * @author Adrian Cole
 * @since 4.0
 */
@ConfigurationElements(elements = {
         @ConfigurationElement(name = "loader", parent = "loaders", 
                  description = "org.infinispan.loaders.s3.S3CacheStore",
                  cardinalityInParent=Cardinality.UNBOUNDED),
         @ConfigurationElement(name = "properties", parent = "loader") })
public class S3CacheStoreConfig extends LockSupportCacheStoreConfig {
   private String awsAccessKey;
   private String awsSecretKey;
   private String bucket;
   private String proxyHost;
   private int proxyPort;
   private long requestTimeout = 10000;


   public long getRequestTimeout() {
      return requestTimeout;
   }

   @ConfigurationProperty(name="requestTimeout",
            parentElement="properties")
   public void setRequestTimeout(long requestTimeout) {
      this.requestTimeout = requestTimeout;
   }


   public int getMaxConnections() {
      return maxConnections;
   }

   @ConfigurationProperty(name="maxConnections",
            parentElement="properties")
   public void setMaxConnections(int maxConnections) {
      this.maxConnections = maxConnections;
   }

   private int maxConnections = 3;
   private boolean secure = true;

   public boolean isSecure() {
      return secure;
   }

   @ConfigurationProperty(name="secure",
            parentElement="properties")
   public void setSecure(boolean secure) {
      this.secure = secure;
   }


   public S3CacheStoreConfig() {
      setCacheLoaderClassName(S3CacheStore.class.getName());
   }

   public String getAwsAccessKey() {
      return awsAccessKey;
   }


   @ConfigurationProperty(name="awsAccessKey",
            parentElement="properties")
   public void setAwsAccessKey(String awsAccessKey) {
      this.awsAccessKey = awsAccessKey;
   }

   public String getAwsSecretKey() {
      return awsSecretKey;
   }


   @ConfigurationProperty(name="awsSecretKey",
            parentElement="properties")
   public void setAwsSecretKey(String awsSecretKey) {
      this.awsSecretKey = awsSecretKey;
   }

   public String getBucket() {
      return bucket;
   }

   @ConfigurationProperty(name="bucket",
            parentElement="properties")
   public void setBucket(String bucket) {
      this.bucket = bucket;
   }

   public String getProxyHost() {
      return proxyHost;
   }

   @ConfigurationProperty(name="proxyHost",
            parentElement="properties")
   public void setProxyHost(String proxyHost) {
      this.proxyHost = proxyHost;
   }

   public int getProxyPort() {
      return proxyPort;
   }

   @ConfigurationProperty(name="proxyPort",
            parentElement="properties")
   public void setProxyPort(int proxyPort) {
      this.proxyPort = proxyPort;
   }


}