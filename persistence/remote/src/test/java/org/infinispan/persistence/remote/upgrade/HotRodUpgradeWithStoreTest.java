package org.infinispan.persistence.remote.upgrade;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.testng.AssertJUnit.assertEquals;

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
   private static final int NUM_ENTRIES = 50;

   @BeforeClass
   public void setup() throws Exception {
      ConfigurationBuilder sourceStoreBuilder = new ConfigurationBuilder();
      sourceStoreBuilder.clustering().cacheMode(CacheMode.DIST_SYNC)
            .locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(true).versioning().scheme(VersioningScheme.SIMPLE).enable()
            .persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .storeName("sourceStore");

      ConfigurationBuilder targetStoreBuilder = new ConfigurationBuilder();
      targetStoreBuilder.clustering().cacheMode(CacheMode.DIST_SYNC)
            .locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(true).versioning().scheme(VersioningScheme.SIMPLE).enable()
            .persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .storeName("targetStore");

      sourceCluster = new TestCluster.Builder().setName("sourceCluster").setNumMembers(2)
            .cache().name(CACHE_NAME).configuredWith(sourceStoreBuilder)
            .build();

      targetCluster = new TestCluster.Builder().setName("targetCluster").setNumMembers(2)
            .cache().name(CACHE_NAME).remotePort(sourceCluster.getHotRodPort()).configuredWith(targetStoreBuilder)
            .build();
   }

   public void testSynchronization() throws Exception {
      RemoteCache<String, String> sourceRemoteCache = sourceCluster.getRemoteCache(CACHE_NAME);
      RemoteCache<String, String> targetRemoteCache = targetCluster.getRemoteCache(CACHE_NAME);

      IntStream.rangeClosed(1, NUM_ENTRIES).boxed().map(String::valueOf)
            .forEach(s -> sourceRemoteCache.put(s, s, 10, TimeUnit.MINUTES, 30, TimeUnit.MINUTES));

      assertEquals("25", targetRemoteCache.get("25"));

      RollingUpgradeManager upgradeManager = targetCluster.getRollingUpgradeManager(CACHE_NAME);
      long count = upgradeManager.synchronizeData("hotrod");
      upgradeManager.disconnectSource("hotrod");

      assertEquals(NUM_ENTRIES, count);
      assertEquals(sourceCluster.getEmbeddedCache(CACHE_NAME).size(), targetCluster.getEmbeddedCache(CACHE_NAME).size());


      MetadataValue<String> metadataValue = targetRemoteCache.getWithMetadata("10");
      assertEquals(10 * 60, metadataValue.getLifespan());
      assertEquals(30 * 60, metadataValue.getMaxIdle());

      assertEquals(NUM_ENTRIES, storeSize(sourceCluster));
      assertEquals(NUM_ENTRIES, storeSize(targetCluster));
   }

   private int storeSize(TestCluster testCluster) {
      PersistenceManager pm = extractComponent(testCluster.getEmbeddedCache(CACHE_NAME), PersistenceManager.class);
      DummyInMemoryStore store = pm.getStores(DummyInMemoryStore.class).iterator().next();
      return store.size();
   }

   @AfterClass
   public void tearDown() {
      targetCluster.destroy();
      sourceCluster.destroy();
   }

}
