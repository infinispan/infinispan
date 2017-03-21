package org.infinispan.persistence.remote.upgrade;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.upgrade.RollingUpgradeManager;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(testName = "upgrade.hotrod.HotRodUpgradeWithStoreTest", groups = "functional")
public class HotRodUpgradeWithStoreTest extends AbstractInfinispanTest {

   private TestCluster sourceCluster, targetCluster;
   private static final String CACHE_NAME = HotRodUpgradeWithStoreTest.class.getName();
   private static final int INITIAL_NUM_ENTRIES = 10;

   @BeforeClass
   public void setup() throws Exception {
      ConfigurationBuilder sourceStoreBuilder = new ConfigurationBuilder();
      sourceStoreBuilder.clustering().cacheMode(CacheMode.DIST_SYNC)
            .locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(true)
            .versioning().scheme(VersioningScheme.SIMPLE).enable()
            .persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class).shared(true).storeName("sourceStore");

      ConfigurationBuilder targetStoreBuilder = new ConfigurationBuilder();
      targetStoreBuilder.clustering().cacheMode(CacheMode.DIST_SYNC)
            .locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(true)
            .versioning().scheme(VersioningScheme.SIMPLE).enable()
            .persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class).shared(true).storeName("targetStore");

      sourceCluster = new TestCluster.Builder().setName("sourceCluster").setNumMembers(1)
            .cache().name(CACHE_NAME).configuredWith(sourceStoreBuilder)
            .build();

      targetCluster = new TestCluster.Builder().setName("targetCluster").setNumMembers(1)
            .cache().name(CACHE_NAME).remotePort(sourceCluster.getHotRodPort()).configuredWith(targetStoreBuilder)
            .build();
   }

   public void testSynchronization() throws Exception {
      RemoteCache<String, String> sourceRemoteCache = sourceCluster.getRemoteCache(CACHE_NAME);
      RemoteCache<String, String> targetRemoteCache = targetCluster.getRemoteCache(CACHE_NAME);

      IntStream.rangeClosed(1, INITIAL_NUM_ENTRIES).boxed().map(String::valueOf)
            .forEach(s -> sourceRemoteCache.put(s, s, 10, TimeUnit.MINUTES, 30, TimeUnit.MINUTES));

      // Check data is persisted in the source cluster
      assertEquals(INITIAL_NUM_ENTRIES, storeWrites(sourceCluster));

      // Verify data is accessible from the target cluster
      assertEquals("4", targetRemoteCache.get("4"));

      // Change data from the target cluster and check it propagates to the source cluster's store
      targetRemoteCache.put("8", "changed", 10, TimeUnit.MINUTES, 30, TimeUnit.MINUTES);
      targetRemoteCache.remove("5");
      targetRemoteCache.remove("1");
      targetRemoteCache.put("new key", "new value", 10, TimeUnit.MINUTES, 30, TimeUnit.MINUTES);
      assertEquals(INITIAL_NUM_ENTRIES - 1, storeSize(sourceCluster));
      assertEquals(INITIAL_NUM_ENTRIES + 2, storeWrites(sourceCluster));

      // Verify modified entries are in the target clusters' store
      assertEquals(2, storeSize(targetCluster));

      // Synchronize data
      RollingUpgradeManager upgradeManager = targetCluster.getRollingUpgradeManager(CACHE_NAME);
      long migrated = upgradeManager.synchronizeData("hotrod",10,1);
      upgradeManager.disconnectSource("hotrod");

      assertEquals(INITIAL_NUM_ENTRIES - 1, migrated);
      assertEquals(sourceCluster.getEmbeddedCache(CACHE_NAME).size(), targetCluster.getEmbeddedCache(CACHE_NAME).size());

      MetadataValue<String> metadataValue = targetRemoteCache.getWithMetadata("10");
      assertEquals(10 * 60, metadataValue.getLifespan());
      assertEquals(30 * 60, metadataValue.getMaxIdle());

      // Verify data correctly migrated
      assertFalse(targetRemoteCache.containsKey("5"));
      assertFalse(targetRemoteCache.containsKey("1"));
      assertEquals("4",targetRemoteCache.get("4"));
      assertEquals("changed", targetRemoteCache.get("8"));
      assertEquals("new value", targetRemoteCache.get("new key"));

      // Source cluster's store should not have changed
      assertEquals(INITIAL_NUM_ENTRIES - 1, storeSize(sourceCluster));

      // Target cluster's store should have the migrated entries
      assertEquals(INITIAL_NUM_ENTRIES - 1, targetRemoteCache.size());
      assertEquals(INITIAL_NUM_ENTRIES - 1, storeSize(targetCluster));

      // No data should be written to the source cluster during migration
      assertEquals(INITIAL_NUM_ENTRIES + 2, storeWrites(sourceCluster));

   }

   private DummyInMemoryStore getDummyStore(TestCluster testCluster) {
      PersistenceManager pm = extractComponent(testCluster.getEmbeddedCache(CACHE_NAME), PersistenceManager.class);
      return pm.getStores(DummyInMemoryStore.class).iterator().next();
   }

   private int storeSize(TestCluster cluster) {
      return getDummyStore(cluster).size();
   }

   private int storeWrites(TestCluster cluster) {
      return getDummyStore(cluster).stats().get("write");
   }

   @AfterClass
   public void tearDown() {
      targetCluster.destroy();
      sourceCluster.destroy();
   }

}
