package org.infinispan.client.hotrod;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Properties;

import static org.testng.AssertJUnit.assertEquals;

/**
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
      cacheManager = TestCacheManagerFactory.createLocalCacheManager();
      cache = cacheManager.getCache();

      hotRodServer = TestHelper.startHotRodServer(cacheManager);

      Properties hotrodClientConf = new Properties();
      hotrodClientConf.put("infinispan.client.hotrod.server_list", "localhost:" + hotRodServer.getPort());
      remoteCacheManager = new RemoteCacheManager(hotrodClientConf);
      remoteCache = remoteCacheManager.getCache();
      populateCacheManager();
      return cacheManager;
   }

   @AfterMethod
   @Override
   protected void clearContent() {
      
   }

   private void populateCacheManager() {
      for (int i = 0; i < 100; i++) {
         remoteCache.put(i, i);
      }
   }

   public void testBulkGet() {
      Map<Object,Object> map = remoteCache.getBulk();
      assert map.size() == 100;
      for (int i = 0; i < 100; i++) {
         assert map.get(i).equals(i);
      }
   }

   public void testBulkGetWithSize() {
      Map<Object,Object> map = remoteCache.getBulk(50);
      assertEquals(50, map.size());
      for (int i = 0; i < 100; i++) {
         if (map.containsKey(i)) {
            Integer value = (Integer) map.get(i);
            assertEquals((Integer)i, value);
         }
      }
   }
}
