package org.infinispan.persistence.remote.upgrade;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.IteratorMapper;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.jboss.marshalling.commons.GenericJBossMarshaller;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.remote.RemoteStore;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.upgrade.RollingUpgradeManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(testName = "upgrade.hotrod.HotRodUpgradeSynchronizerTest", groups = "functional")
public class HotRodUpgradeSynchronizerTest extends AbstractInfinispanTest {

   protected TestCluster sourceCluster, targetCluster;

   protected static final String OLD_CACHE = "old-cache";
   protected String TEST_CACHE = this.getClass().getName();

   protected static final ProtocolVersion OLD_PROTOCOL_VERSION = ProtocolVersion.PROTOCOL_VERSION_31;
   protected static final ProtocolVersion NEW_PROTOCOL_VERSION = ProtocolVersion.DEFAULT_PROTOCOL_VERSION;

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
       if (!reuseClustersAcrossMethods()) {
          // Original behaviour: build fresh clusters for every test method.
          sourceCluster = createSourceCluster();
          targetCluster = configureTargetCluster();
          return;
       }

       // Reuse the same clusters across all test methods of this class instead of rebuilding them on each @BeforeMethod cycle.
       // The OS may not release a JGroups multicast socket immediately after a channel disconnect, so repeatedly binding/unbinding
       // the same address (one per method) intermittently fails with "BindException: Address already in use". Building once and only
       // resetting state avoids that churn entirely. Tests that add their migration remote store dynamically re-establish it via
       // connectTargetCluster(), which each test invokes before synchronizing.
        if (sourceCluster == null) {
           sourceCluster = createSourceCluster();
           targetCluster = configureTargetCluster();
        } else {
           // A previous test's disconnectSource cleared the migration remote store(s); re-establish them so this method can sync.
           sourceCluster.cleanAllCaches();
           targetCluster.cleanAllCaches();
           reconnectMigration(targetCluster);
        }
    }

    /**
     * Whether the source and target clusters should be created once per class (in {@link #setup()}) and reused across all test methods,
     * or rebuilt for every method. Reuse is enabled by default: it avoids repeatedly binding/unbinding the same JGroups multicast socket
     * on each @BeforeMethod cycle. Subclasses that override this to return false keep rebuilding per method instead.
     */
    protected boolean reuseClustersAcrossMethods() {
       return true;
    }

    /**
     * Re-establishes the migration path on a reused target cluster before running a test method, after a previous test's disconnectSource
     * cleared it. The default re-adds plain (non-SSL) remote stores for both caches pointing at the source HotRod port. Subclasses that use
     * SSL or named remote containers override this with their own store configuration; subclasses that add stores dynamically in
     * {@link #connectTargetCluster()} override it to do nothing, since each test re-adds them itself.
     */
    protected void reconnectMigration(TestCluster target) {
       target.connectSource(OLD_CACHE, buildRemoteStoreConfig(OLD_CACHE, OLD_PROTOCOL_VERSION));
       target.connectSource(TEST_CACHE, buildRemoteStoreConfig(TEST_CACHE, NEW_PROTOCOL_VERSION));
    }

    private StoreConfiguration buildRemoteStoreConfig(String cacheName, ProtocolVersion version) {
       ConfigurationBuilder builder = new ConfigurationBuilder();
       RemoteStoreConfigurationBuilder store = builder.persistence().addStore(RemoteStoreConfigurationBuilder.class);
       store.remoteCacheName(cacheName).protocolVersion(version).shared(true).segmented(false)
             .addServer().host("localhost").port(sourceCluster.getHotRodPort());
       return store.build().persistence().stores().get(0);
    }

    protected TestCluster createSourceCluster() throws Exception {
       return new TestCluster.Builder().setName("sourceCluster").setNumMembers(2)
             .cache().name(OLD_CACHE)
             .cache().name(TEST_CACHE)
             .build();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
       if (!reuseClustersAcrossMethods()) {
          destroyClusters();
       }
    }

    @AfterClass(alwaysRun = true)
    public void destroyClusters() {
       if (targetCluster != null) {
          targetCluster.destroy();
          targetCluster = null;
       }
       if (sourceCluster != null) {
          sourceCluster.destroy();
          sourceCluster = null;
       }
    }

   private void fillCluster(TestCluster cluster, String cacheName) {
      for (char ch = 'A'; ch <= 'Z'; ch++) {
         String s = Character.toString(ch);
         cluster.getRemoteCache(cacheName).put(s, s, 20, TimeUnit.SECONDS, 30, TimeUnit.SECONDS);
      }
   }

   protected TestCluster configureTargetCluster() {
      return new TestCluster.Builder().setName("targetCluster").setNumMembers(2)
            .cache().name(OLD_CACHE).remotePort(sourceCluster.getHotRodPort()).remoteProtocolVersion(OLD_PROTOCOL_VERSION).remoteStoreProperty(RemoteStore.MIGRATION, "true")
            .cache().name(TEST_CACHE).remotePort(sourceCluster.getHotRodPort()).remoteProtocolVersion(NEW_PROTOCOL_VERSION).remoteStoreProperty(RemoteStore.MIGRATION, "true")
            .build();
   }

   protected void connectTargetCluster() {
      // No op, target cluster is already connected to the source (static remote store added).
   }

   public void testSynchronization() {
      connectTargetCluster();
      RemoteCache<String, String> sourceRemoteCache = sourceCluster.getRemoteCache(TEST_CACHE);
      RemoteCache<String, String> targetRemoteCache = targetCluster.getRemoteCache(TEST_CACHE);

      for (char ch = 'A'; ch <= 'Z'; ch++) {
         String s = Character.toString(ch);
         sourceRemoteCache.put(s, s, 30, TimeUnit.SECONDS, 20, TimeUnit.SECONDS);
      }

      // Verify access to some of the data from the new cluster
      assertEquals("A", targetRemoteCache.get("A"));

      RollingUpgradeManager upgradeManager = targetCluster.getRollingUpgradeManager(TEST_CACHE);
      long count = upgradeManager.synchronizeData("hotrod");

      assertEquals(26, count);
      assertEquals(sourceCluster.getEmbeddedCache(TEST_CACHE).size(), targetCluster.getEmbeddedCache(TEST_CACHE).size());

      upgradeManager.disconnectSource("hotrod");

      MetadataValue<String> metadataValue = targetRemoteCache.getWithMetadata("Z");
      assertEquals(30, metadataValue.getLifespan());
      assertEquals(20, metadataValue.getMaxIdle());

   }

   public void testSynchronizationWithClientDeleteBefore() throws Exception {
      connectTargetCluster();
      // fill source cluster with data
      fillCluster(sourceCluster, TEST_CACHE);

      // Change data in the target cluster
      RemoteCache<String, String> remoteCache = targetCluster.getRemoteCache(TEST_CACHE);
      remoteCache.remove("G");

      assertFalse(remoteCache.containsKey("G"));

      // Perform rolling upgrade
      RollingUpgradeManager rum = targetCluster.getRollingUpgradeManager(TEST_CACHE);
      rum.synchronizeData("hotrod");
      rum.disconnectSource("hotrod");

      // Verify data is consistent
      assertFalse(remoteCache.containsKey("G"));
      assertEquals("A", remoteCache.get("A"));
      assertEquals("U", remoteCache.get("U"));
   }

   public void testSynchronizationWithClientWriteBefore() throws Exception {
      connectTargetCluster();
      // fill source cluster with data
      fillCluster(sourceCluster, TEST_CACHE);

      // Writes data in the target cluster
      RemoteCache<String, String> remoteCache = targetCluster.getRemoteCache(TEST_CACHE);
      remoteCache.put("U", "I");
      remoteCache.put("a", "a");

      assertEquals("a", remoteCache.get("a"));
      assertEquals("I", remoteCache.get("U"));

      // Perform rolling upgrade
      RollingUpgradeManager rum = targetCluster.getRollingUpgradeManager(TEST_CACHE);
      rum.synchronizeData("hotrod");
      rum.disconnectSource("hotrod");

      // Verify data is consistent
      assertEquals("a", remoteCache.get("a"));
      assertEquals("I", remoteCache.get("U"));
   }

   public void testSynchronizationWithClientReadsBefore() throws Exception {
      connectTargetCluster();
      // fill source cluster with data
      fillCluster(sourceCluster, TEST_CACHE);

      // Read data in the target cluster
      RemoteCache<String, String> remoteCache = targetCluster.getRemoteCache(TEST_CACHE);
      assertEquals("X", remoteCache.get("X"));

      // Perform rolling upgrade
      RollingUpgradeManager rum = targetCluster.getRollingUpgradeManager(TEST_CACHE);
      rum.synchronizeData("hotrod");
      rum.disconnectSource("hotrod");

      // Verify data is consistent
      assertEquals("X", remoteCache.get("X"));
   }

   @Test
   public void testSynchronizationWithInFlightUpdates() throws Exception {
      connectTargetCluster();
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
      connectTargetCluster();
      // fill source cluster with data
      fillCluster(sourceCluster, TEST_CACHE);

      RemoteCache<String, String> remoteCache = targetCluster.getRemoteCache(TEST_CACHE);

      doWhenSourceIterationReaches("L", targetCluster, TEST_CACHE, key -> remoteCache.remove("L"));

      RollingUpgradeManager rum = targetCluster.getRollingUpgradeManager(TEST_CACHE);
      rum.synchronizeData("hotrod");
      rum.disconnectSource("hotrod");

      // Verify data is not re-added
      assertNull(remoteCache.get("L"));
      assertEquals("A", remoteCache.get("A"));
      assertEquals("I", remoteCache.get("I"));
   }

   public void testSynchronizationWithInFlightReads() throws Exception {
      connectTargetCluster();
      // fill source cluster with data
      fillCluster(sourceCluster, TEST_CACHE);

      RemoteCache<String, String> remoteCache = targetCluster.getRemoteCache(TEST_CACHE);

      doWhenSourceIterationReaches("G", targetCluster, TEST_CACHE, key -> remoteCache.get("G"));

      RollingUpgradeManager rum = targetCluster.getRollingUpgradeManager(TEST_CACHE);
      rum.synchronizeData("hotrod");
      rum.disconnectSource("hotrod");

      assertEquals("G", remoteCache.get("G"));
   }


   private void doWhenSourceIterationReaches(String key, TestCluster cluster, String cacheName, IterationCallBack callback) {
      cluster.getEmbeddedCaches(cacheName).forEach(c -> {
         PersistenceManager pm = extractComponent(c, PersistenceManager.class);
         RemoteStore<?, ?> remoteStore = pm.getStores(RemoteStore.class).iterator().next();
         RemoteCacheImpl<?, ?> remoteCache = TestingUtil.extractField(remoteStore, "remoteCache");
         RemoteCacheImpl<?, ?> spy = spy(remoteCache);
         doAnswer(invocation -> {
            Object[] params = invocation.getArguments();
            CloseableIterator<Map.Entry<Object, MetadataValue<Object>>> iterator = remoteCache.retrieveEntriesWithMetadata((Set<Integer>) params[0], (int) params[1]);
            Marshaller marshaller = new GenericJBossMarshaller();
            return new IteratorMapper<>(iterator, entry -> {
               try {
                  if (key.equals(marshaller.objectFromByteBuffer((byte[]) entry.getKey()))) {
                     callback.iterationReached(key);
                  }
               } catch (IOException | ClassNotFoundException ex) {
                  throw new RuntimeException(ex);
               }
               return entry;
            });
         }).when(spy).retrieveEntriesWithMetadata(anySet(), anyInt());
         TestingUtil.replaceField(spy, "remoteCache", remoteStore, RemoteStore.class);
      });
   }

}
