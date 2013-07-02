package org.infinispan.loaders.bdbje.configuration;

import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.loaders.bdbje.BdbjeCacheStore;
import org.infinispan.commons.util.TypedProperties;

/**
 * BdbjeCacheStoreConfigurationBuilder. Configures a {@link BdbjeCacheStore}
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class BdbjeCacheStoreConfigurationBuilder extends
      AbstractStoreConfigurationBuilder<BdbjeCacheStoreConfiguration, BdbjeCacheStoreConfigurationBuilder> {
   private String location = "Infinispan-BdbjeCacheStore";
   private long lockAcquistionTimeout = 60 * 1000;
   private int maxTxRetries = 5;
   private String cacheDbNamePrefix;
   private String catalogDbName;
   private String expiryDbPrefix;
   private String environmentPropertiesFile;

   public BdbjeCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public BdbjeCacheStoreConfigurationBuilder self() {
      return this;
   }

   /**
    * A location on disk where the store can write internal files. This defaults to
    * <tt>Infinispan-BdbjeCacheStore</tt> in the current working directory.
    *
    * @return
    */
   public BdbjeCacheStoreConfigurationBuilder location(String location) {
      this.location = location;
      return this;
   }

   /**
    * The length of time, in milliseconds, to wait for locks before timing out and throwing an
    * exception. By default, this is set to <tt>60000</tt>.
    *
    * @param lockAcquistionTimeout
    * @return
    */
   public BdbjeCacheStoreConfigurationBuilder lockAcquistionTimeout(long lockAcquistionTimeout) {
      this.lockAcquistionTimeout = lockAcquistionTimeout;
      return this;
   }

   /**
    * The number of times transaction prepares will attempt to resolve a deadlock before throwing an
    * exception. By default, this is set to <tt>5</tt>.
    *
    * @param maxTxRetries
    * @return
    */
   public BdbjeCacheStoreConfigurationBuilder maxTxRetries(int maxTxRetries) {
      this.maxTxRetries = maxTxRetries;
      return this;
   }

   /**
    * The prefix to add before the cache name to generate the filename of the SleepyCat database
    * persisting this store. If unspecified, the filename defaults to
    * <tt>{@link org.infinispan.Cache#getName()} cache#name}</tt>.
    *
    * @param cacheDbNamePrefix
    * @return
    */
   public BdbjeCacheStoreConfigurationBuilder cacheDbNamePrefix(String cacheDbNamePrefix) {
      this.cacheDbNamePrefix = cacheDbNamePrefix;
      return this;
   }

   /**
    * The name of the SleepyCat database persisting the class information for objects in this store.
    * This defaults to <tt>{@link org.infinispan.Cache#getName()} cache#name}_class_catalog</tt>.
    *
    * @param catalogDbName
    * @return
    */
   public BdbjeCacheStoreConfigurationBuilder catalogDbName(String catalogDbName) {
      this.catalogDbName = catalogDbName;
      return this;
   }

   /**
    * The prefix to add before the cache name to generate the filename of the SleepyCat database
    * persisting this store containing the expiration entries. If unspecified, the filename defaults to
    * <tt>{@link org.infinispan.Cache#getName()} cache#name}_expiry</tt>.
    *
    * @param expiryDbPrefix
    * @return
    */
   public BdbjeCacheStoreConfigurationBuilder expiryDbPrefix(String expiryDbPrefix) {
      this.expiryDbPrefix = expiryDbPrefix;
      return this;
   }

   /**
    * The name of the SleepyCat properties file containing <tt>je.*</tt> properties to initialize
    * the JE environment. Defaults to null, no properties are passed in to the JE engine if this is
    * null or empty. The file specified needs to be available on the classpath, or must be an
    * absolute path to a valid properties file. Refer to SleepyCat JE Environment configuration
    * documentation for details.
    *
    * @param environmentPropertiesFile
    * @return
    */
   public BdbjeCacheStoreConfigurationBuilder environmentPropertiesFile(String environmentPropertiesFile) {
      this.environmentPropertiesFile = environmentPropertiesFile;
      return this;
   }

   @Override
   public BdbjeCacheStoreConfiguration create() {
      return new BdbjeCacheStoreConfiguration(location, lockAcquistionTimeout, maxTxRetries, cacheDbNamePrefix,
            catalogDbName, expiryDbPrefix, environmentPropertiesFile, purgeOnStartup, purgeSynchronously,
            purgerThreads, fetchPersistentState, ignoreModifications, TypedProperties.toTypedProperties(properties),
            async.create(), singletonStore.create());
   }

   @Override
   public BdbjeCacheStoreConfigurationBuilder read(BdbjeCacheStoreConfiguration template) {

      this.location = template.location();
      this.lockAcquistionTimeout = template.lockAcquisitionTimeout();
      this.maxTxRetries = template.maxTxRetries();
      this.cacheDbNamePrefix = template.cacheDbNamePrefix();
      this.catalogDbName = template.catalogDbName();
      this.expiryDbPrefix = template.expiryDbPrefix();
      this.environmentPropertiesFile = template.environmentPropertiesFile();

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
