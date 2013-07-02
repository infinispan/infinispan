package org.infinispan.client.hotrod;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.*;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

/**
 * Tests functionality related to getting multiple entries from a HotRod server
 * in bulk.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "client.hotrod.BulkGetSimpleTest", groups = "functional")
public class BulkGetSimpleTest extends SingleCacheManagerTest {
   private HotRodServer hotRodServer;
   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<Object, Object> remoteCache;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(
            hotRodCacheConfiguration());
      cache = cacheManager.getCache();

      hotRodServer = TestHelper.startHotRodServer(cacheManager);

      Properties hotrodClientConf = new Properties();
      hotrodClientConf.put("infinispan.client.hotrod.server_list", "localhost:" + hotRodServer.getPort());
      remoteCacheManager = new RemoteCacheManager(hotrodClientConf);
      remoteCache = remoteCacheManager.getCache();
      return cacheManager;
   }

   @AfterTest
   public void release() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotRodServer);
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
