package org.infinispan.loaders.cloud.configuration;

import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.LegacyConfigurationAdaptor;
import org.infinispan.configuration.cache.LegacyLoaderAdapter;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.loaders.cloud.CloudCacheStoreConfig;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.util.TypedProperties;

@BuiltBy(CloudCacheStoreConfigurationBuilder.class)
public class CloudCacheStoreConfiguration extends AbstractStoreConfiguration implements
      LegacyLoaderAdapter<CloudCacheStoreConfig> {

   private final String identity;
   private final String password;
   private final String bucketPrefix;
   private final String proxyHost;
   private final int proxyPort;
   private final long requestTimeout;
   private final boolean lazyPurgingOnly;
   private final String cloudService;
   private final int maxConnections;
   private final boolean secure;
   private final boolean compress;
   private final String cloudServiceLocation;

   CloudCacheStoreConfiguration(String identity, String password, String bucketPrefix, String proxyHost, int proxyPort,
         long requestTimeout, boolean lazyPurgingOnly, String cloudService, String cloudServiceLocation, int maxConnections, boolean secure,
         boolean compress, boolean purgeOnStartup, boolean purgeSynchronously, int purgerThreads,
         boolean fetchPersistentState, boolean ignoreModifications, TypedProperties properties,
         AsyncStoreConfiguration asyncStoreConfiguration, SingletonStoreConfiguration singletonStoreConfiguration) {
      super(purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState, ignoreModifications, properties,
            asyncStoreConfiguration, singletonStoreConfiguration);
      this.identity = identity;
      this.password = password;
      this.bucketPrefix = bucketPrefix;
      this.proxyHost = proxyHost;
      this.proxyPort = proxyPort;
      this.requestTimeout = requestTimeout;
      this.lazyPurgingOnly = lazyPurgingOnly;
      this.cloudService = cloudService;
      this.cloudServiceLocation = cloudServiceLocation;
      this.maxConnections = maxConnections;
      this.secure = secure;
      this.compress = compress;
   }

   @Override
   public CloudCacheStoreConfig adapt() {
      CloudCacheStoreConfig config = new CloudCacheStoreConfig();

      LegacyConfigurationAdaptor.adapt(this, config);

      config.setIdentity(identity);
      config.setPassword(password);
      config.setBucketPrefix(bucketPrefix);
      config.setProxyHost(proxyHost);
      config.setProxyPort(Integer.toString(proxyPort));
      config.setRequestTimeout(requestTimeout);
      config.setLazyPurgingOnly(lazyPurgingOnly);
      config.setCloudService(cloudService);
      config.setCloudServiceLocation(cloudServiceLocation);
      config.setMaxConnections(maxConnections);
      config.setSecure(secure);
      config.setCompress(compress);

      return config;
   }

   public String bucketPrefix() {
      return bucketPrefix;
   }

   public String cloudService() {
      return cloudService;
   }

   public String cloudServiceLocation() {
      return cloudServiceLocation;
   }

   public boolean compress() {
      return compress;
   }

   public String identity() {
      return identity;
   }

   public boolean lazyPurgingOnly() {
      return lazyPurgingOnly;
   }

   public int maxConnections() {
      return maxConnections;
   }

   public String password() {
      return password;
   }

   public String proxyHost() {
      return proxyHost;
   }

   public int proxyPort() {
      return proxyPort;
   }

   public long requestTimeout() {
      return requestTimeout;
   }

   public boolean secure() {
      return secure;
   }

}
