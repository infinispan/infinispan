package org.infinispan.client.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(testName = "client.hotrod.ForceReturnValuesTest", groups = "functional")
@CleanupAfterMethod
public class ForceReturnValuesTest extends SingleCacheManagerTest {
   private HotRodServer hotRodServer;
   private RemoteCacheManager remoteCacheManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      cache = cacheManager.getCache();

      hotRodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
              HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host(hotRodServer.getHost()).port(hotRodServer.getPort());
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      return cacheManager;
   }

   @AfterMethod
   void shutdown() {
      HotRodClientTestingUtil.killRemoteCacheManager(remoteCacheManager);
      HotRodClientTestingUtil.killServers(hotRodServer);
      hotRodServer = null;
   }

   public void testDontForceReturnValues() {
      RemoteCache<String, String> rc = remoteCacheManager.getCache();
      String rv = rc.put("Key", "Value");
      assert rv == null;
      rv = rc.put("Key", "Value2");
      assert rv == null;
   }

   public void testForceReturnValues() {
      RemoteCache<String, String> rc = remoteCacheManager.getCache();
      String rv = rc.put("Key", "Value");
      assertNull(rv);
      rv = rc.withFlags(Flag.FORCE_RETURN_VALUE).put("Key", "Value2");
      assertEquals("Value", rv);
   }
}
