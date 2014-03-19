package org.infinispan.client.hotrod;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.*;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.*;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(groups = "functional", testName = "client.hotrod.CacheContainerTest")
public class CacheContainerTest extends SingleCacheManagerTest {

   private static final String CACHE_NAME = "someName";

   EmbeddedCacheManager cacheManager = null;
   HotRodServer hotrodServer = null;
   RemoteCacheManager remoteCacheManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(
            hotRodCacheConfiguration());
      cacheManager.defineConfiguration(CACHE_NAME, hotRodCacheConfiguration().build());
      hotrodServer = TestHelper.startHotRodServer(cacheManager);
      remoteCacheManager = new RemoteCacheManager("localhost:" + hotrodServer.getPort(), true);
      return cacheManager;
   }

   @AfterTest
   public void release() {
      killCacheManagers(cacheManager);
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotrodServer);
   }

   public void testObtainingSameInstanceMultipleTimes() {
      RemoteCache<Object, Object> objectCache = remoteCacheManager.getCache();
      RemoteCache<Object, Object> objectCache2 = remoteCacheManager.getCache();
      assert objectCache == objectCache2;
   }

   public void testObtainingSameInstanceMultipleTimes2() {
      RemoteCache<Object, Object> objectCache = remoteCacheManager.getCache(CACHE_NAME);
      RemoteCache<Object, Object> objectCache2 = remoteCacheManager.getCache(CACHE_NAME);
      assert objectCache == objectCache2;
   }
}
