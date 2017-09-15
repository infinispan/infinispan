package org.infinispan.configuration;

import static org.infinispan.configuration.cache.CacheMode.DIST_SYNC;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "configuration.ConfigurationOverrideTest")
public class ConfigurationOverrideTest extends AbstractInfinispanTest {

   private EmbeddedCacheManager cm;

   @AfterMethod
   public void stopCacheManager() {
      cm.stop();
   }

   public void testConfigurationOverride() throws Exception {
      ConfigurationBuilder defaultCfgBuilder = new ConfigurationBuilder();
      defaultCfgBuilder.memory().size(200).storageType(StorageType.BINARY);

      cm = TestCacheManagerFactory.createCacheManager(defaultCfgBuilder);
      final ConfigurationBuilder cacheCfgBuilder =
            new ConfigurationBuilder().read(defaultCfgBuilder.build());
      cm.defineConfiguration("my-cache", cacheCfgBuilder.build());
      Cache<?, ?> cache = cm.getCache("my-cache");
      assertEquals(200, cache.getCacheConfiguration().memory().size());
      assertEquals(StorageType.BINARY, cache.getCacheConfiguration().memory().storageType());
   }

   public void testSimpleDistributedClusterModeDefault() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(DIST_SYNC)
            .hash().numOwners(3).numSegments(51);

      cm = TestCacheManagerFactory.createClusteredCacheManager(builder);
      cm.defineConfiguration("my-cache", builder.build());

      Cache<?, ?> cache = cm.getCache("my-cache");
      // These are all overridden values
      ClusteringConfiguration clusteringCfg =
            cache.getCacheConfiguration().clustering();
      assertEquals(DIST_SYNC, clusteringCfg.cacheMode());
      assertEquals(3, clusteringCfg.hash().numOwners());
      assertEquals(51, clusteringCfg.hash().numSegments());
   }

   public void testSimpleDistributedClusterModeNamedCache() throws Exception {
      final String cacheName = "my-cache";
      final Configuration config = new ConfigurationBuilder()
            .clustering().cacheMode(DIST_SYNC)
            .hash().numOwners(3).numSegments(51).build();

      cm = TestCacheManagerFactory.createClusteredCacheManager();
      cm.defineConfiguration(cacheName, config);
      Cache<?, ?> cache = cm.getCache(cacheName);
      ClusteringConfiguration clusteringCfg =
            cache.getCacheConfiguration().clustering();
      assertEquals(DIST_SYNC, clusteringCfg.cacheMode());
      assertEquals(3, clusteringCfg.hash().numOwners());
      assertEquals(51, clusteringCfg.hash().numSegments());
   }

   public void testOverrideWithStore() {
      final ConfigurationBuilder builder1 = new ConfigurationBuilder();
      builder1.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
      cm = TestCacheManagerFactory.createCacheManager(builder1);
      ConfigurationBuilder builder2 = new ConfigurationBuilder();
      builder2.read(cm.getDefaultCacheConfiguration());
      builder2.memory().size(1000);
      Configuration configuration = cm.defineConfiguration("named", builder2.build());
      assertEquals(1, configuration.persistence().stores().size());
   }

   public void testPartialOverride() {
      ConfigurationBuilder baseBuilder = new ConfigurationBuilder();
      baseBuilder.memory().size(200).storageType(StorageType.BINARY);
      Configuration base = baseBuilder.build();
      ConfigurationBuilder overrideBuilder = new ConfigurationBuilder();
      overrideBuilder.read(base).locking().concurrencyLevel(31);
      Configuration override = overrideBuilder.build();
      assertEquals(200, base.memory().size());
      assertEquals(200, override.memory().size());
      assertEquals(StorageType.BINARY, base.memory().storageType());
      assertEquals(StorageType.BINARY, override.memory().storageType());
      assertEquals(32, base.locking().concurrencyLevel());
      assertEquals(31, override.locking().concurrencyLevel());
   }

   public void testConfigurationUndefine() {
      cm = TestCacheManagerFactory.createCacheManager();
      cm.defineConfiguration("testConfig", new ConfigurationBuilder().build());
      cm.undefineConfiguration("testConfig");
      assertNull(cm.getCacheConfiguration("testConfig"));
   }

   @Test(expectedExceptions=IllegalStateException.class)
   public void testConfigurationUndefineWhileInUse() {
      cm = TestCacheManagerFactory.createCacheManager();
      cm.defineConfiguration("testConfig", new ConfigurationBuilder().build());
      cm.getCache("testConfig");
      cm.undefineConfiguration("testConfig");
   }

}
