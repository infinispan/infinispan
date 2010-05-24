package org.infinispan.client.hotrod;

import org.infinispan.manager.CacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static junit.framework.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "client.hotrod.ServerShutdownTest", groups = "functional")
public class ServerShutdownTest {

   public void testServerShutdownWithConnectedClient() {
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createLocalCacheManager();
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
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createLocalCacheManager();
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
