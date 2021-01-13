package org.infinispan.persistence.remote.upgrade;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT_TYPE;
import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.upgrade.RollingUpgradeManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test for Hot Rod Rolling Upgrades from caches storing application/x-java-object.
 *
 * @since 12.0
 */
@Test(testName = "upgrade.hotrod.HotRodUpgradePojoTest", groups = "functional")
public class HotRodUpgradePojoTest extends AbstractInfinispanTest {
   protected TestCluster sourceCluster, targetCluster;

   protected static final String CACHE_NAME = "theCache";

   public static final int ENTRIES = 50;

   @BeforeMethod
   public void setup() throws Exception {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.clustering().cacheMode(CacheMode.DIST_SYNC).hash().numSegments(256);
      config.encoding().key().mediaType(APPLICATION_OBJECT_TYPE);
      config.encoding().value().mediaType(APPLICATION_OBJECT_TYPE);

      sourceCluster = new TestCluster.Builder().setName("sourceCluster").setNumMembers(2)
            .marshaller(ProtoStreamMarshaller.class)
            .ctx(SerializationCtx.INSTANCE)
            .cache().name(CACHE_NAME).configuredWith(config)
            .build();

      targetCluster = new TestCluster.Builder().setName("targetCluster").setNumMembers(2)
            .marshaller(ProtoStreamMarshaller.class)
            .ctx(SerializationCtx.INSTANCE)
            .cache().name(CACHE_NAME).configuredWith(config)
            .remotePort(sourceCluster.getHotRodPort())
            .remoteStoreRawValues(false)
            .remoteStoreWrapping(false)
            .build();
   }

   public void testSynchronization() throws Exception {
      RemoteCache<Object, Object> sourceRemoteCache = sourceCluster.getRemoteCache(CACHE_NAME);
      RemoteCache<Object, Object> targetRemoteCache = targetCluster.getRemoteCache(CACHE_NAME);

      // Populate source cluster
      for (int i = 0; i < ENTRIES; i++) {
         sourceRemoteCache.put(i, new CustomObject("text", i), 20, TimeUnit.MINUTES, 30, TimeUnit.MINUTES);
      }

      assertEquals(ENTRIES, sourceRemoteCache.size());

      // Verify data access from the target cluster
      assertEquals(ENTRIES, targetRemoteCache.size());
      assertEquals(new CustomObject("text", 2), targetRemoteCache.get(2));

      // Do a Rolling Upgrade
      RollingUpgradeManager upgradeManager = targetCluster.getRollingUpgradeManager(CACHE_NAME);
      long count = upgradeManager.synchronizeData("hotrod");
      assertEquals(ENTRIES, count);

      // Disconnect remote store
      upgradeManager.disconnectSource("hotrod");

      // Check migrated data
      assertEquals(sourceCluster.getEmbeddedCache(CACHE_NAME).size(), targetCluster.getEmbeddedCache(CACHE_NAME).size());
      assertEquals(new CustomObject("text", 10), targetRemoteCache.get(10));
      MetadataValue<Object> metadataValue = targetRemoteCache.getWithMetadata(ENTRIES - 1);
      assertEquals(20 * 60, metadataValue.getLifespan());
      assertEquals(30 * 60, metadataValue.getMaxIdle());
   }

   public void testSynchronizationBetweenEmbedded() throws Exception {
      sourceCluster.cleanAllCaches();
      targetCluster.cleanAllCaches();

      Cache<Object, Object> sourceCache = sourceCluster.getEmbeddedCache(CACHE_NAME);
      Cache<Object, Object> targetCache = targetCluster.getEmbeddedCache(CACHE_NAME);

      // Populate source cluster
      for (int i = 0; i < ENTRIES; i++) {
         sourceCache.put(i, new CustomObject("text", i), 20, TimeUnit.MINUTES, 30, TimeUnit.MINUTES);
      }

      assertEquals(ENTRIES, sourceCache.size());

      // Verify data access from the target cluster
      assertEquals(ENTRIES, targetCache.size());
      assertEquals(new CustomObject("text", 2), targetCache.get(2));

      // Do a Rolling Upgrade
      RollingUpgradeManager upgradeManager = targetCluster.getRollingUpgradeManager(CACHE_NAME);
      long count = upgradeManager.synchronizeData("hotrod");
      assertEquals(ENTRIES, count);

      // Disconnect remote store
      upgradeManager.disconnectSource("hotrod");

      // Check migrated data
      assertEquals(sourceCluster.getEmbeddedCache(CACHE_NAME).size(), targetCluster.getEmbeddedCache(CACHE_NAME).size());
      assertEquals(new CustomObject("text", 10), targetCache.get(10));
      CacheEntry<Object, Object> entry = targetCache.getAdvancedCache().getCacheEntry(ENTRIES - 1);
      assertEquals(20 * 60 * 1000, entry.getLifespan());
      assertEquals(30 * 60 * 1000, entry.getMaxIdle());

   }

   @AfterMethod
   public void tearDown() {
      sourceCluster.destroy();
      targetCluster.destroy();
   }

}
