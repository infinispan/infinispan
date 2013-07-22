package org.infinispan.client.hotrod;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

/**
 * Tests functionality related to getting multiple entries from a HotRod server
 * in bulk.
 * 
 * @author Jiri Holusa [jholusa@redhat.com]
 * @since 5.2
 */

@Test(groups = "functional")
public abstract class BaseBulkGetTest extends MultipleCacheManagersTest {
   protected HotRodServer[] hotrodServers;
   protected RemoteCacheManager remoteCacheManager;
   protected RemoteCache<Object, Object> remoteCache;

   abstract protected int numberOfHotRodServers();

   abstract protected ConfigurationBuilder clusterConfig();

   @Override
   protected void createCacheManagers() throws Throwable {
      final int numServers = numberOfHotRodServers();
      hotrodServers = new HotRodServer[numServers];

      createCluster(hotRodCacheConfiguration(clusterConfig()), numberOfHotRodServers());

      for (int i = 0; i < numServers; i++) {
         EmbeddedCacheManager cm = cacheManagers.get(i);
         hotrodServers[i] = TestHelper.startHotRodServer(cm);
      }

      String servers = TestHelper.getServersString(hotrodServers);

      remoteCacheManager = new RemoteCacheManager(servers);
      remoteCache = remoteCacheManager.getCache();
   }

   @AfterClass
   public void release() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotrodServers);
   }

   private void populateCacheManager() {
      for (int i = 0; i < 100; i++) {
         remoteCache.put(i, i);
      }
   }

   public void testBulkGet() {
      populateCacheManager();
      Map<Object,Object> map = remoteCache.getBulk();
      assert map.size() == 100;
      for (int i = 0; i < 100; i++) {
         assert map.get(i).equals(i);
      }
      assert map.size() == 100;

   }

   public void testBulkGetWithSize() {
      populateCacheManager();
      Map<Object,Object> map = remoteCache.getBulk(50);
      assertEquals(50, map.size());
      for (int i = 0; i < 100; i++) {
         if (map.containsKey(i)) {
            Integer value = (Integer) map.get(i);
            assertEquals((Integer)i, value);
         }
      }
   }

   public void testBulkGetAfterLifespanExpire() throws InterruptedException {
      Map dataIn = new HashMap();
      dataIn.put("aKey", "aValue");
      dataIn.put("bKey", "bValue");
      final long startTime = System.currentTimeMillis();
      final long lifespan = 10000;
      remoteCache.putAll(dataIn, lifespan, TimeUnit.MILLISECONDS);

      Map dataOut = new HashMap();
      while (true) {
         dataOut = remoteCache.getBulk();
         if (System.currentTimeMillis() >= startTime + lifespan)
            break;
         assertEquals(dataIn, dataOut);
         Thread.sleep(100);
      }

      // Make sure that in the next 30 secs data is removed
      while (System.currentTimeMillis() < startTime + lifespan + 30000) {
         dataOut = remoteCache.getBulk();
         if (dataOut.size() == 0) return;
      }

      assert dataOut.size() == 0 :
            String.format("Data not empty, it contains: %s elements", dataOut.size());
   }

}
