package org.infinispan.loaders.bdbje.configuration;

import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.LegacyConfigurationAdaptor;
import org.infinispan.configuration.cache.LegacyLoaderAdapter;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.loaders.bdbje.BdbjeCacheStoreConfig;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.util.TypedProperties;

@BuiltBy(BdbjeCacheStoreConfigurationBuilder.class)
public class BdbjeCacheStoreConfiguration extends AbstractStoreConfiguration implements
      LegacyLoaderAdapter<BdbjeCacheStoreConfig> {

   private final String location;
   private final long lockAcquistionTimeout;
   private final int maxTxRetries;
   private final String cacheDbNamePrefix;
   private final String catalogDbName;
   private final String expiryDbPrefix;
   private final String environmentPropertiesFile;

   public BdbjeCacheStoreConfiguration(String location, long lockAcquistionTimeout, int maxTxRetries,
         String cacheDbNamePrefix, String catalogDbName, String expiryDbPrefix, String environmentPropertiesFile,
         boolean purgeOnStartup, boolean purgeSynchronously, int purgerThreads, boolean fetchPersistentState,
         boolean ignoreModifications, TypedProperties properties, AsyncStoreConfiguration asyncStoreConfiguration,
         SingletonStoreConfiguration singletonStoreConfiguration) {
      super(purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState, ignoreModifications, properties,
            asyncStoreConfiguration, singletonStoreConfiguration);
      this.location = location;
      this.lockAcquistionTimeout = lockAcquistionTimeout;
      this.maxTxRetries = maxTxRetries;
      this.cacheDbNamePrefix = cacheDbNamePrefix;
      this.catalogDbName = catalogDbName;
      this.expiryDbPrefix = expiryDbPrefix;
      this.environmentPropertiesFile = environmentPropertiesFile;

   }

   public String location() {
      return location;
   }

   public long lockAcquisitionTimeout() {
      return lockAcquistionTimeout;
   }

   public int maxTxRetries() {
      return maxTxRetries;
   }

   public String cacheDbNamePrefix() {
      return cacheDbNamePrefix;
   }

   public String catalogDbName() {
      return catalogDbName;
   }

   public String expiryDbPrefix() {
      return expiryDbPrefix;
   }

   public String environmentPropertiesFile() {
      return environmentPropertiesFile;
   }

   @Override
   public BdbjeCacheStoreConfig adapt() {
      BdbjeCacheStoreConfig config = new BdbjeCacheStoreConfig();

      LegacyConfigurationAdaptor.adapt(this, config);

      config.setCacheDbNamePrefix(cacheDbNamePrefix);
      config.setCatalogDbName(catalogDbName);
      config.setEnvironmentPropertiesFile(environmentPropertiesFile);
      config.setExpiryDbNamePrefix(expiryDbPrefix);
      config.setLocation(location);
      config.setLockAcquistionTimeout(lockAcquistionTimeout);
      config.setMaxTxRetries(maxTxRetries);

      return config;
   }

}
