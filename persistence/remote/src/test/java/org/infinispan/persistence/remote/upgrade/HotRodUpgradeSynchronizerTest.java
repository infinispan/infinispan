package org.infinispan.persistence.remote.upgrade;

import static org.infinispan.client.hotrod.ProtocolVersion.DEFAULT_PROTOCOL_VERSION;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.remote.RemoteStore;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.upgrade.RollingUpgradeManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(testName = "upgrade.hotrod.HotRodUpgradeSynchronizerTest", groups = "functional")
public class HotRodUpgradeSynchronizerTest extends AbstractInfinispanTest {

   protected TestCluster sourceCluster, targetCluster;

   protected static final String OLD_CACHE = "old-cache";
   protected static final String TEST_CACHE = HotRodUpgradeSynchronizerTest.class.getName();

   protected static final String OLD_PROTOCOL_VERSION = "2.0";
   protected static final String NEW_PROTOCOL_VERSION = DEFAULT_PROTOCOL_VERSION.toString();

   @BeforeMethod
   public void setup() throws Exception {
      sourceCluster = new TestCluster.Builder().setName("sourceCluster").setNumMembers(1)
            .cache().name(OLD_CACHE)
            .cache().name(TEST_CACHE)
            .build();

      targetCluster = new TestCluster.Builder().setName("targetCluster").setNumMembers(1)
            .cache().name(OLD_CACHE).remotePort(sourceCluster.getHotRodPort()).remoteProtocolVersion(OLD_PROTOCOL_VERSION)
            .cache().name(TEST_CACHE).remotePort(sourceCluster.getHotRodPort()).remoteProtocolVersion(NEW_PROTOCOL_VERSION)
            .build();
   }

   private void fillCluster(TestCluster cluster, String cacheName) {
      for (char ch = 'A'; ch <= 'Z'; ch++) {
         String s = Character.toString(ch);
         cluster.getRemoteCache(cacheName).put(s, s, 20, TimeUnit.SECONDS, 30, TimeUnit.SECONDS);
      }
   }

   public void testSynchronization() throws Exception {
      RemoteCache<String, String> sourceRemoteCache = sourceCluster.getRemoteCache(TEST_CACHE);
      RemoteCache<String, String> targetRemoteCache = targetCluster.getRemoteCache(TEST_CACHE);

      for (char ch = 'A'; ch <= 'Z'; ch++) {
         String s = Character.toString(ch);
         sourceRemoteCache.put(s, s, 20, TimeUnit.SECONDS, 30, TimeUnit.SECONDS);
      }

      // Verify access to some of the data from the new cluster
      assertEquals("A", targetRemoteCache.get("A"));

      RollingUpgradeManager upgradeManager = targetCluster.getRollingUpgradeManager(TEST_CACHE);
      long count = upgradeManager.synchronizeData("hotrod");

      assertEquals(26, count);
      assertEquals(sourceCluster.getEmbeddedCache(TEST_CACHE).size(), targetCluster.getEmbeddedCache(TEST_CACHE).size());

      upgradeManager.disconnectSource("hotrod");

      MetadataValue<String> metadataValue = targetRemoteCache.getWithMetadata("Z");
      assertEquals(20, metadataValue.getLifespan());
      assertEquals(30, metadataValue.getMaxIdle());

   }

   public void testSynchronizationWithClientChanges() throws Exception {
      // fill source cluster with data
      fillCluster(sourceCluster, TEST_CACHE);

      // Change data in the target cluster
      RemoteCache<String, String> remoteCache = targetCluster.getRemoteCache(TEST_CACHE);
      remoteCache.remove("G");
      remoteCache.put("U", "I");
      remoteCache.put("a", "a");

      assertFalse(remoteCache.containsKey("G"));
      assertEquals("a", remoteCache.get("a"));
      assertEquals("I", remoteCache.get("U"));

      // Perform rolling upgrade
      RollingUpgradeManager rum = targetCluster.getRollingUpgradeManager(TEST_CACHE);
      rum.synchronizeData("hotrod");
      rum.disconnectSource("hotrod");

      // Verify data is consistent
      assertFalse(remoteCache.containsKey("G"));
      assertEquals("a", remoteCache.get("a"));
      assertEquals("I", remoteCache.get("U"));
   }


   @Test
   public void testSynchronizationWithInFlightUpdates() throws Exception {
      // fill source cluster with data
      fillCluster(sourceCluster, TEST_CACHE);

      RemoteCache<String, String> remoteCache = targetCluster.getRemoteCache(TEST_CACHE);

      doWhenSourceIterationReaches("M", targetCluster, TEST_CACHE, key -> remoteCache.put("M", "changed"));

      RollingUpgradeManager rum = targetCluster.getRollingUpgradeManager(TEST_CACHE);
      rum.synchronizeData("hotrod");
      rum.disconnectSource("hotrod");

      // Verify data is not overridden
      assertEquals("changed", remoteCache.get("M"));
   }

   @Test
   public void testSynchronizationWithInFlightDeletes() throws Exception {
      // fill source cluster with data
      fillCluster(sourceCluster, TEST_CACHE);

      RemoteCache<String, String> remoteCache = targetCluster.getRemoteCache(TEST_CACHE);

      doWhenSourceIterationReaches("L", targetCluster, TEST_CACHE, key -> remoteCache.remove("L"));

      RollingUpgradeManager rum = targetCluster.getRollingUpgradeManager(TEST_CACHE);
      rum.synchronizeData("hotrod");
      rum.disconnectSource("hotrod");

      // Verify data is not re-added
      assertNull(remoteCache.get("L"));
   }


   private void doWhenSourceIterationReaches(String key, TestCluster cluster, String cacheName, IterationCallBack callback) {
      cluster.getEmbeddedCaches(cacheName).forEach(c -> {
         PersistenceManager pm = extractComponent(c, PersistenceManager.class);
         RemoteStore remoteStore = pm.getStores(RemoteStore.class).iterator().next();
         RemoteCacheImpl remoteCache = TestingUtil.extractField(remoteStore, "remoteCache");
         RemoteCacheImpl spy = spy(remoteCache);
         doAnswer(invocation -> {
            Object[] params = invocation.getArguments();
            CallbackRemoteIterator<Object> remoteCloseableIterator = new CallbackRemoteIterator<>(spy.getOperationsFactory(), (int) params[1], null, true, spy.getDataFormat());
            remoteCloseableIterator.addCallback(callback, key);
            remoteCloseableIterator.start();
            return remoteCloseableIterator;
         }).when(spy).retrieveEntriesWithMetadata(isNull(), anyInt());
         TestingUtil.replaceField(spy, "remoteCache", remoteStore, RemoteStore.class);
      });
   }

   @AfterMethod
   public void tearDown() {
      sourceCluster.destroy();
      targetCluster.destroy();
   }

}
