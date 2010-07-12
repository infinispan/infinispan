package org.infinispan.client.hotrod;

import org.infinispan.config.Configuration;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "client.hotrod.HotRodServerStartStopTest", groups = "functional")
public class HotRodServerStartStopTest extends MultipleCacheManagersTest {
   private HotRodServer hotRodServer1;
   private HotRodServer hotRodServer2;

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC);
      addClusterEnabledCacheManager(config);
      addClusterEnabledCacheManager(config);

      hotRodServer1 = TestHelper.startHotRodServer(manager(0));
      hotRodServer2 = TestHelper.startHotRodServer(manager(1));

      assert manager(0).getCache() != null;
      assert manager(1).getCache() != null;

      TestingUtil.blockUntilViewReceived(manager(0).getCache(), 2, 10000);
      TestingUtil.blockUntilCacheStatusAchieved(manager(0).getCache(), ComponentStatus.RUNNING, 10000);
      TestingUtil.blockUntilCacheStatusAchieved(manager(1).getCache(), ComponentStatus.RUNNING, 10000);

      cache(0).put("k","v");
      assertEquals("v", cache(1).get("k"));
   }

   public void testTouchServer() {
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager("localhost", hotRodServer1.getPort(), true);
      RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache();
      remoteCache.put("k", "v");
      assertEquals("v", remoteCache.get("k"));
   }

   @Test (dependsOnMethods = "testTouchServer")
   public void testHrServerStop() {
      hotRodServer1.stop();
      hotRodServer2.stop();
   }
}
