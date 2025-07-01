package org.infinispan.configuration;

import static org.infinispan.configuration.cache.CacheMode.DIST_SYNC;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import org.infinispan.Cache;
import org.infinispan.commons.configuration.Combine;
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
      defaultCfgBuilder.memory().maxCount(200).storage(StorageType.HEAP);

      cm = TestCacheManagerFactory.createCacheManager(defaultCfgBuilder);
      final ConfigurationBuilder cacheCfgBuilder =
            new ConfigurationBuilder().read(defaultCfgBuilder.build(), Combine.DEFAULT);
      cm.defineConfiguration("my-cache", cacheCfgBuilder.build());
      Cache<?, ?> cache = cm.getCache("my-cache");
      assertEquals(200, cache.getCacheConfiguration().memory().maxCount());
      assertEquals(StorageType.HEAP, cache.getCacheConfiguration().memory().storage());
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
      builder2.read(cm.getDefaultCacheConfiguration(), Combine.DEFAULT);
      builder2.memory().maxCount(1000);
      Configuration configuration = cm.defineConfiguration("named", builder2.build());
      assertEquals(1, configuration.persistence().stores().size());
   }

   public void testPartialOverride() {
      ConfigurationBuilder baseBuilder = new ConfigurationBuilder();
      baseBuilder.memory().maxCount(200).storage(StorageType.HEAP);
      Configuration base = baseBuilder.build();
      ConfigurationBuilder overrideBuilder = new ConfigurationBuilder();
      overrideBuilder.read(base, Combine.DEFAULT).locking().concurrencyLevel(31);
      Configuration override = overrideBuilder.build();
      assertEquals(200, base.memory().maxCount());
      assertEquals(200, override.memory().maxCount());
      assertEquals(StorageType.HEAP, base.memory().storage());
      assertEquals(StorageType.HEAP, override.memory().storage());
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
