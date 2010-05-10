package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.config.Configuration;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.CacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(groups = "functional", testName = "client.hotrod.CSAIntegrationTest")
public class CSAIntegrationTest extends MultipleCacheManagersTest {

   private HotRodServer hotRodServer1;
   private HotRodServer hotRodServer2;
   private HotRodServer hotRodServer3;
   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<Object, Object> remoteCache;
   private TcpTransportFactory tcpConnectionFactory;
   private static final String CACHE_NAME = "a";

   @AfterMethod
   @Override
   protected void clearContent() throws Throwable {
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      cleanup = CleanupPhase.AFTER_METHOD;
      Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC);
      CacheManager cm1 = addClusterEnabledCacheManager();
      CacheManager cm2 = addClusterEnabledCacheManager();
      CacheManager cm3 = addClusterEnabledCacheManager();
      cm1.defineConfiguration(CACHE_NAME, config);
      cm2.defineConfiguration(CACHE_NAME, config);
      cm3.defineConfiguration(CACHE_NAME, config);

//
      BaseDistFunctionalTest.RehashWaiter.waitForInitRehashToComplete(manager(0).getCache(CACHE_NAME), manager(1).getCache(CACHE_NAME), manager(2).getCache(CACHE_NAME));

      hotRodServer1 = TestHelper.startHotRodServer(manager(0));
      hotRodServer2 = TestHelper.startHotRodServer(manager(1));
      hotRodServer3 = TestHelper.startHotRodServer(manager(2));


      manager(0).getCache(CACHE_NAME).put("k","v");
      manager(0).getCache(CACHE_NAME).get("k").equals("v");
      manager(1).getCache(CACHE_NAME).get("k").equals("v");
      manager(2).getCache(CACHE_NAME).get("k").equals("v");



      log.info("Local replication test passed!");

      //Important: this only connects to one of the two servers!
      remoteCacheManager = new RemoteCacheManager("localhost:" + hotRodServer2.getPort() + ";localhost:" + hotRodServer2.getPort());
      remoteCache = remoteCacheManager.getCache();

      tcpConnectionFactory = (TcpTransportFactory) TestingUtil.extractField(remoteCacheManager, "transportFactory");
   }

//   public void testPing() {
//      assert tcpConnectionFactory.getServers().size() == 1;
//      remoteCache.ping();
//      assert tcpConnectionFactory.getServers().size() == 3;
//   }

   public void testHashInfoRetrieved() {
      remoteCache.put("k", "v");
   }
}
