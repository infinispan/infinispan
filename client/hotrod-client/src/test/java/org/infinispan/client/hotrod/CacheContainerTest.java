package org.infinispan.client.hotrod;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(groups = "functional", testName = "client.hotrod.CacheContainerTest")
public class CacheContainerTest extends SingleCacheManagerTest {

   private static final String CACHE_NAME = "someName";

   CacheManager cacheManager = null;
   HotRodServer hotrodServer = null;
   RemoteCacheManager remoteCacheManager;

   @Override
   protected CacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createLocalCacheManager();
      cacheManager.defineConfiguration(CACHE_NAME, new Configuration());
      hotrodServer = TestHelper.startHotRodServer(cacheManager);
      remoteCacheManager = new RemoteCacheManager("localhost:" + hotrodServer.getPort(), true);
      return cacheManager;
   }

   @AfterTest(alwaysRun = true)
   public void release() {
      if (cacheManager != null) cacheManager.stop();
      if (hotrodServer != null) hotrodServer.stop();
   }

   public void testObtainingSameInstanceMultipleTimes() {
      Cache<Object,Object> objectCache = remoteCacheManager.getCache();
      Cache<Object,Object> objectCache2 = remoteCacheManager.getCache();
      assert objectCache == objectCache2;
   }

   public void testObtainingSameInstanceMultipleTimes2() {
      Cache<Object,Object> objectCache = remoteCacheManager.getCache(CACHE_NAME);
      Cache<Object,Object> objectCache2 = remoteCacheManager.getCache(CACHE_NAME);
      assert objectCache == objectCache2;
   }
}
