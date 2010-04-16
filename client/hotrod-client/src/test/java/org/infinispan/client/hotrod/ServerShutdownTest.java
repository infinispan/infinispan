package org.infinispan.client.hotrod;

import org.infinispan.manager.CacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static junit.framework.Assert.assertEquals;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "client.hotrod.ServerShutdownTest", groups = "functional")
public class ServerShutdownTest {

   public void testServerShutdownWithConnectedClient() {
      CacheManager cacheManager = TestCacheManagerFactory.createLocalCacheManager();
      HotRodServer hotrodServer = TestHelper.startHotRodServer(cacheManager);
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager("localhost", hotrodServer.getPort());
      RemoteCache remoteCache = remoteCacheManager.getCache();

      remoteCache.put("k","v");
      assertEquals("v", remoteCache.get("k"));

      hotrodServer.stop();
      cacheManager.stop();
      remoteCacheManager.stop();
   }

   public void testServerShutdownWithoutConnectedClient() {
      CacheManager cacheManager = TestCacheManagerFactory.createLocalCacheManager();
      HotRodServer hotrodServer = TestHelper.startHotRodServer(cacheManager);
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager("localhost", hotrodServer.getPort());
      RemoteCache remoteCache = remoteCacheManager.getCache();

      remoteCache.put("k","v");
      assertEquals("v", remoteCache.get("k"));

      remoteCacheManager.stop();
      hotrodServer.stop();
      cacheManager.stop();
   }
}
