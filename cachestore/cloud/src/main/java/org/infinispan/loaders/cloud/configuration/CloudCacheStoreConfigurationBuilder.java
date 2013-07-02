package org.infinispan.loaders.cloud.configuration;

import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.loaders.cloud.CloudCacheStore;
import org.infinispan.commons.util.TypedProperties;

/**
 * CloudCacheStoreConfigurationBuilder. Configures a {@link CloudCacheStore}
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class CloudCacheStoreConfigurationBuilder extends
      AbstractStoreConfigurationBuilder<CloudCacheStoreConfiguration, CloudCacheStoreConfigurationBuilder> {
   private String bucketPrefix;
   private String cloudService;
   private String cloudServiceLocation;
   private boolean compress = true;
   private String identity;
   private boolean lazyPurgingOnly = false;
   private int maxConnections = 10000;
   private String password;
   private String proxyHost;
   private int proxyPort;
   private long requestTimeout = 10000;
   private boolean secure = true;

   public CloudCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public CloudCacheStoreConfigurationBuilder self() {
      return this;
   }

   /**
    * A String that is prepended to generated buckets or containers on the cloud store. Buckets or
    * containers are named {bucketPrefix}-{cacheName}
    */
   public CloudCacheStoreConfigurationBuilder bucketPrefix(String bucketPrefix) {
      this.bucketPrefix = bucketPrefix;
      return this;
   }

   /**
    * The cloud service to use. Supported values are dependent on the included version of the
    * JClouds library.
    */
   public CloudCacheStoreConfigurationBuilder cloudService(String cloudService) {
      this.cloudService = cloudService;
      return this;
   }

   /**
    * The data center to use. Note that this is specific to the cloud provider in question. E.g.,
    * Amazon's S3 service supports storage buckets in several different locations. Optional, and
    * defaults to <tt>DEFAULT</tt>.
    */
   public CloudCacheStoreConfigurationBuilder cloudServiceLocation(String cloudServiceLocation) {
      this.cloudServiceLocation = cloudServiceLocation;
      return this;
   }

   /**
    * Whether to compress stored data. Defaults to <tt>true</tt>.
    */
   public CloudCacheStoreConfigurationBuilder compress(boolean compress) {
      this.compress = compress;
      return this;
   }

   /**
    * A String that identifies you to the cloud provider. For example. with AWS, this is your ACCESS
    * KEY.
    */
   public CloudCacheStoreConfigurationBuilder identity(String identity) {
      this.identity = identity;
      return this;
   }

   /**
    * If enabled, then expired entries are only purged on access, lazily, rather than by using the
    * periodic eviction thread. Defaults to <tt>false</tt>.
    */
   public CloudCacheStoreConfigurationBuilder lazyPurgingOnly(boolean lazyPurgingOnly) {
      this.lazyPurgingOnly = lazyPurgingOnly;
      return this;
   }

   /**
    * The maximum number of concurrent connections to make to the cloud provider. Defaults to
    * <tt>10</tt>.
    */
   public CloudCacheStoreConfigurationBuilder maxConnections(int maxConnections) {
      this.maxConnections = maxConnections;
      return this;
   }

   /**
    * A String that is used to authenticate you with the cloud provider. For example. with AWS, this
    * is your SECRET KEY.
    */
   public CloudCacheStoreConfigurationBuilder password(String password) {
      this.password = password;
      return this;
   }

   /**
    * The host name of a proxy to use. Optional, no proxy is used if this is un-set.
    */
   public CloudCacheStoreConfigurationBuilder proxyHost(String proxyHost) {
      this.proxyHost = proxyHost;
      return this;
   }

   /**
    * The port of a proxy to use.
    */
   public CloudCacheStoreConfigurationBuilder proxyPort(int proxyPort) {
      this.proxyPort = proxyPort;
      return this;
   }

   /**
    * A timeout to use when communicating with the cloud storage provider, in milliseconds. Defaults to <tt>10000</tt>.
    */
   public CloudCacheStoreConfigurationBuilder requestTimeout(long requestTimeout) {
      this.requestTimeout = requestTimeout;
      return this;
   }

   /**
    * Whether to use secure (SSL) connections or not. Defaults to <tt>true</tt>.
    */
   public CloudCacheStoreConfigurationBuilder secure(boolean secure) {
      this.secure = secure;
      return this;
   }

   @Override
   public CloudCacheStoreConfiguration create() {
      return new CloudCacheStoreConfiguration(identity, password, bucketPrefix, proxyHost, proxyPort, requestTimeout,
            lazyPurgingOnly, cloudService, cloudServiceLocation, maxConnections, secure, compress, purgeOnStartup,
            purgeSynchronously, purgerThreads, fetchPersistentState, ignoreModifications,
            TypedProperties.toTypedProperties(properties), async.create(), singletonStore.create());
   }

   @Override
   public CloudCacheStoreConfigurationBuilder read(CloudCacheStoreConfiguration template) {
      bucketPrefix = template.bucketPrefix();
      cloudService = template.cloudService();
      cloudServiceLocation = template.cloudServiceLocation();
      compress = template.compress();
      identity = template.identity();
      lazyPurgingOnly = template.lazyPurgingOnly();
      maxConnections = template.maxConnections();
      password = template.password();
      proxyHost = template.proxyHost();
      proxyPort = template.proxyPort();
      requestTimeout = template.requestTimeout();
      secure = template.secure();

      // AbstractStore-specific configuration
      fetchPersistentState = template.fetchPersistentState();
      ignoreModifications = template.ignoreModifications();
      properties = template.properties();
      purgeOnStartup = template.purgeOnStartup();
      purgeSynchronously = template.purgeSynchronously();
      async.read(template.async());
      singletonStore.read(template.singletonStore());
      return this;
   }

}
