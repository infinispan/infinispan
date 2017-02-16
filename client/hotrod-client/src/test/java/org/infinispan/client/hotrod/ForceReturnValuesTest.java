package org.infinispan.client.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNotSame;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertSame;

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
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder.addServer().host("localhost").port(hotRodServer.getPort());
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      return cacheManager;
   }

   @AfterMethod
   void shutdown() {
      HotRodClientTestingUtil.killRemoteCacheManager(remoteCacheManager);
      HotRodClientTestingUtil.killServers(hotRodServer);
   }

   public void testDontForceReturnValues() {
      RemoteCache<String, String> rc = remoteCacheManager.getCache();
      String rv = rc.put("Key", "Value");
      assert rv == null;
      rv = rc.put("Key", "Value2");
      assert rv == null;
   }

   public void testForceReturnValues() {
      RemoteCache<String, String> rc = remoteCacheManager.getCache(true);
      String rv = rc.put("Key", "Value");
      assert rv == null;
      rv = rc.put("Key", "Value2");
      assert rv != null;
      assert "Value".equals(rv);
   }

   public void testSameInstanceForSameForceReturnValues() {
      RemoteCache<String, String> rcDontForceReturn = remoteCacheManager.getCache(false);
      RemoteCache<String, String> rcDontForceReturn2 = remoteCacheManager.getCache(false);
      assertSame("RemoteCache instances should be the same", rcDontForceReturn, rcDontForceReturn2);

      RemoteCache<String, String> rcForceReturn = remoteCacheManager.getCache(true);
      RemoteCache<String, String> rcForceReturn2 = remoteCacheManager.getCache(true);
      assertSame("RemoteCache instances should be the same", rcForceReturn, rcForceReturn2);
   }

   public void testDifferentInstancesForDifferentForceReturnValues() {
      RemoteCache<String, String> rcDontForceReturn = remoteCacheManager.getCache(false);
      RemoteCache<String, String> rcForceReturn = remoteCacheManager.getCache(true);
      assertNotSame("RemoteCache instances should not be the same", rcDontForceReturn, rcForceReturn);

      String rv = rcDontForceReturn.put("Key", "Value");
      assertNull(rv);
      rv = rcDontForceReturn.put("Key", "Value2");
      assertNull(rv);

      rv = rcForceReturn.put("Key2", "Value");
      assertNull(rv);
      rv = rcForceReturn.put("Key2", "Value2");
      assertNotNull(rv);
      assertEquals("Previous value should be 'Value'", "Value", rv);
   }
}
