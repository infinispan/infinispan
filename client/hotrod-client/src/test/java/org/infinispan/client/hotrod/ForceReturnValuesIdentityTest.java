package org.infinispan.client.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(testName = "client.hotrod.ForceReturnValuesIdentityTest", groups = "functional")
@CleanupAfterMethod
public class ForceReturnValuesIdentityTest extends SingleCacheManagerTest {

   private HotRodServer hotRodServer;
   private RemoteCacheManager remoteCacheManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      cache = cacheManager.getCache();

      hotRodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
         HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("localhost").port(hotRodServer.getPort());
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      return cacheManager;
   }

   @AfterMethod
   void shutdown() {
      HotRodClientTestingUtil.killRemoteCacheManager(remoteCacheManager);
      HotRodClientTestingUtil.killServers(hotRodServer);
      hotRodServer = null;
   }

   public void testDifferentInstancesForDifferentForceReturnValues() {
      RemoteCache<String, String> rcDontForceReturn = remoteCacheManager.getCache();
      RemoteCache<String, String> rcForceReturn = rcDontForceReturn.withFlags(Flag.FORCE_RETURN_VALUE);
      assertNotSame(rcDontForceReturn, rcForceReturn, "RemoteCache instances should not be the same");

      String rv = rcDontForceReturn.put("Key", "Value");
      assertNull(rv);
      rv = rcDontForceReturn.put("Key", "Value2");
      assertNull(rv);

      rv = rcForceReturn.put("Key2", "Value");
      assertNull(rv);
      rv = rcForceReturn.put("Key2", "Value2");
      assertNotNull(rv);
      assertEquals("Value", rv, "Previous value should be 'Value'");
   }
}
