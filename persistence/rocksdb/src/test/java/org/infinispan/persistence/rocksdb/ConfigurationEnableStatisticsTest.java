package org.infinispan.persistence.rocksdb;

import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.Cache;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.persistence.Store;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.rocksdb.configuration.RocksDBExpirationConfiguration;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfiguration;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.rocksdb.config.ConfigurationEnableStatisticsTest")
public class ConfigurationEnableStatisticsTest extends AbstractInfinispanTest {

   private String tmpDirectory = TestingUtil.tmpDirectory(this.getClass());

   public void testRocksDbEnableStatistics() {

      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      global.globalState().persistentLocation(tmpDirectory);
      global.defaultCacheName("defaultCache");

      ConfigurationBuilder cb = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      cb.persistence()
         .addStore(RocksDBStoreStatisticsConfigurationBuilder.class)
         .enableStatistics(true)
         .location(tmpDirectory)
         .expiredLocation(tmpDirectory);

      EmbeddedCacheManager cacheManager = new DefaultCacheManager(global.build(), new ConfigurationBuilder().build());
      cacheManager.defineConfiguration("weather", cb.build());
      Cache<String, String> cache = cacheManager.getCache("weather");
      cache.put("foo", "bar");

      RocksDBStatisticsStore store = TestingUtil.getFirstLoader(cache);
      assertTrue(store.getDbStatistics().getTickerCount(TickerType.BYTES_WRITTEN) > 0);
   }

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
   }

   @ConfigurationFor(RocksDBStatisticsStore.class)
   @BuiltBy(RocksDBStoreStatisticsConfigurationBuilder.class)
   public static class RocksDBStoreStatisticsConfiguration extends RocksDBStoreConfiguration {

      final static AttributeDefinition<Boolean> ENABLE_STATISTICS = AttributeDefinition.builder("enableStatistics", false, Boolean.class).immutable().build();

      public static AttributeSet attributeDefinitionSet() {
         return new AttributeSet(RocksDBStoreStatisticsConfiguration.class, RocksDBStoreConfiguration.attributeDefinitionSet(), ENABLE_STATISTICS);
      }

      private final Attribute<Boolean> enableStatistics;

      public RocksDBStoreStatisticsConfiguration(AttributeSet attributes, AsyncStoreConfiguration async, RocksDBExpirationConfiguration expiration) {
         super(attributes, async, expiration);
         this.enableStatistics = attributes.attribute(ENABLE_STATISTICS);
      }

      public boolean enableStatistics() {
         return enableStatistics.get();
      }
   }

   public static class RocksDBStoreStatisticsConfigurationBuilder extends RocksDBStoreConfigurationBuilder {

      public RocksDBStoreStatisticsConfigurationBuilder(PersistenceConfigurationBuilder builder) {
         super(builder, RocksDBStoreStatisticsConfiguration.attributeDefinitionSet());
      }

      public RocksDBStoreStatisticsConfigurationBuilder enableStatistics(boolean b) {
         attributes.attribute(RocksDBStoreStatisticsConfiguration.ENABLE_STATISTICS).set(b);
         return this;
      }

      @Override
      public RocksDBStoreConfiguration create() {
         return new RocksDBStoreStatisticsConfiguration(attributes.protect(), async.create(), expiration.create());
      }
   }

   @Store
   @ConfiguredBy(RocksDBStoreStatisticsConfiguration.class)
   public static class RocksDBStatisticsStore extends RocksDBStore {
      private Statistics dbStatistics;
      private Statistics expiredDbStatistics;
      @Override
      protected DBOptions dataDbOptions() {
         DBOptions options = super.dataDbOptions();
         if (isEnableStatistics()) {
            this.dbStatistics = new Statistics();
            options.setStatistics(this.dbStatistics);
         }
         return options;
      }

      @Override
      protected Options expiredDbOptions() {
         Options options = super.expiredDbOptions();
         if (isEnableStatistics()) {
            this.expiredDbStatistics = new Statistics();
            options.setStatistics(this.expiredDbStatistics);
         }
         return options;
      }

      private boolean isEnableStatistics() {
         RocksDBStoreStatisticsConfiguration storeStatisticsConfiguration = (RocksDBStoreStatisticsConfiguration) configuration;
         return storeStatisticsConfiguration.enableStatistics();
      }

      public Statistics getDbStatistics() {
         return dbStatistics;
      }

      public Statistics getExpiredDbStatistics() {
         return expiredDbStatistics;
      }
   }
}