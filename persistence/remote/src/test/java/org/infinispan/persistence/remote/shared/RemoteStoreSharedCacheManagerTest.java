package org.infinispan.persistence.remote.shared;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.remote.RemoteStore;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "persistence.remote.shared.RemoteStoreSharedCacheManagerTest")
public class RemoteStoreSharedCacheManagerTest extends AbstractInfinispanTest {

   private static final int PORT = 19721;
   private static final String CACHE_NAME = "remote-cache-name";
   private static final String CONFIG_PATH = "remotestore-shared-container.xml";
   private HotRodServer hotRodServer;
   private EmbeddedCacheManager storeCacheManager;

   @BeforeClass
   protected void setup() {
      storeCacheManager = TestCacheManagerFactory.createCacheManager();
      storeCacheManager.createCache(CACHE_NAME, hotRodCacheConfiguration().build());
      hotRodServer = HotRodTestingUtil.startHotRodServer(storeCacheManager, PORT);
   }

   @AfterClass
   protected void tearDown() {
      HotRodClientTestingUtil.killServers(hotRodServer);
      hotRodServer = null;
      TestingUtil.killCacheManagers(storeCacheManager);
      storeCacheManager = null;
   }

   @Test
   public void testOperationsSharedStore() throws Exception {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml(CONFIG_PATH)) {
         @Override
         public void call() {
            Cache<Object, Object> c0 = cm.getCache("RemoteStoreWithDefaultContainer");
            Cache<Object, Object> c1 = cm.getCache("AnotherRemoteStore");

            c0.put("k", "v");
            assertEquals(c1.get("k"), "v");

            List<RemoteStore> rs0 = new ArrayList<>(extractRemoteStore(c0));
            List<RemoteStore> rs1 = new ArrayList<>(extractRemoteStore(c1));

            assertEquals(rs0.size(), 1);
            assertEquals(rs0.size(), rs1.size());

            RemoteCacheManager rcm0 = TestingUtil.extractField(rs0.get(0), "remoteCacheManager");
            RemoteCacheManager rcm1 = TestingUtil.extractField(rs1.get(0), "remoteCacheManager");

            assertSame(rcm0, rcm1);
         }
      });
   }

   private Set<RemoteStore> extractRemoteStore(Cache<Object, Object> cache) {
      PersistenceManager pm = ComponentRegistry.componentOf(cache, PersistenceManager.class);
      return pm.getStores(RemoteStore.class);
   }
}
