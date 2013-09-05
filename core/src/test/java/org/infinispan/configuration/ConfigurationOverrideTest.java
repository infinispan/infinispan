package org.infinispan.configuration;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.infinispan.eviction.EvictionStrategy.*;
import static org.infinispan.configuration.cache.CacheMode.*;

@Test(groups = "functional", testName = "configuration.ConfigurationOverrideTest")
public class ConfigurationOverrideTest extends AbstractInfinispanTest {

   private EmbeddedCacheManager cm;

   @AfterMethod
   public void stopCacheManager() {
      cm.stop();
   }

   public void testConfigurationOverride() throws Exception {
      ConfigurationBuilder defaultCfgBuilder = new ConfigurationBuilder();
      defaultCfgBuilder.eviction().maxEntries(200).strategy(LIRS);

      cm = TestCacheManagerFactory.createCacheManager(defaultCfgBuilder);
      final ConfigurationBuilder cacheCfgBuilder =
            new ConfigurationBuilder().read(defaultCfgBuilder.build());
      cm.defineConfiguration("my-cache", cacheCfgBuilder.build());
      Cache<?, ?> cache = cm.getCache("my-cache");
      assertEquals(200,
            cache.getCacheConfiguration().eviction().maxEntries());
      assertEquals(LIRS,
            cache.getCacheConfiguration().eviction().strategy());
   }

   public void testSimpleDistributedClusterModeDefault() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(DIST_SYNC)
            .hash().numOwners(3).numSegments(51);

      cm = TestCacheManagerFactory.createClusteredCacheManager(builder);

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
      cm = new DefaultCacheManager(new GlobalConfigurationBuilder().build(), builder1.build());
      ConfigurationBuilder builder2 = new ConfigurationBuilder();
      builder2.read(cm.getDefaultCacheConfiguration());
      builder2.eviction().maxEntries(1000);
      Configuration configuration = cm.defineConfiguration("named", builder2.build());
      assertEquals(1, configuration.persistence().stores().size());
   }

}