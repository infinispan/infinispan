package org.infinispan.persistence.remote.upgrade;

import static java.util.stream.IntStream.range;
import static org.infinispan.test.AbstractCacheTest.getDefaultClusteredCacheConfig;
import static org.testng.Assert.assertEquals;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.upgrade.RollingUpgradeManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * Test rolling upgrades with different key encodings in the server, e.g. StorageType.BINARY
 *
 * @since 11.0
 */
@Test(groups = "functional", testName = "upgrade.hotrod.HotRodUpgradeEncodingsTest")
public class HotRodUpgradeEncodingsTest extends AbstractInfinispanTest {

   protected static final String CACHE_NAME = "encoded";

   protected TestCluster sourceCluster, targetCluster;

   protected StorageType storageType;

   @Factory
   public Object[] factory() {
      return new Object[]{
            new HotRodUpgradeEncodingsTest().withStorage(StorageType.HEAP),
            new HotRodUpgradeEncodingsTest().withStorage(StorageType.OFF_HEAP)
      };
   }

   @Override
   protected String parameters() {
      return "[" + storageType.toString() + "]";
   }

   public HotRodUpgradeEncodingsTest withStorage(StorageType storageType) {
      this.storageType = storageType;
      return this;
   }

   @BeforeClass
   public void setup() {
      ConfigurationBuilder configurationBuilder = getConfigurationBuilder();
      sourceCluster = new TestCluster.Builder().setName("sourceCluster").setNumMembers(2)
            .cache().name(CACHE_NAME)
            .configuredWith(configurationBuilder)
            .build();

      targetCluster = configureTargetCluster();
   }

   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder configurationBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      configurationBuilder.clustering().hash().numSegments(2);
      configurationBuilder.memory().storage(storageType);
      return configurationBuilder;
   }

   protected TestCluster configureTargetCluster() {
      return new TestCluster.Builder().setName("targetCluster").setNumMembers(2)
            .cache().name(CACHE_NAME).remotePort(sourceCluster.getHotRodPort())
            .configuredWith(getConfigurationBuilder()).build();
   }

   @AfterClass
   public void tearDown() {
      targetCluster.destroy();
      sourceCluster.destroy();
   }


   void loadSourceCluster(int entries) {
      RemoteCache<String, String> remoteCache = sourceCluster.getRemoteCache(CACHE_NAME);
      range(0, entries).boxed().map(String::valueOf).forEach(k -> remoteCache.put(k, "value" + k));
   }

   protected void connectTargetCluster() {
      // No op, target cluster is already connected to the source (static remote store added).
   }

   @Test
   public void testMigrate() throws Exception {
      connectTargetCluster();

      int entries = 1000;
      loadSourceCluster(entries);

      RollingUpgradeManager rum = targetCluster.getRollingUpgradeManager(CACHE_NAME);
      rum.synchronizeData("hotrod", 10, 2);
      targetCluster.disconnectSource(CACHE_NAME);

      assertEquals(targetCluster.getRemoteCache(CACHE_NAME).size(), entries);
   }

}
