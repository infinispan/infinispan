package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
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
   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<Object, Object> remoteCache;
   private TcpTransportFactory tcpConnectionFactory;


   @Override
   protected void createCacheManagers() throws Throwable {
      cleanup = CleanupPhase.AFTER_METHOD;
      Configuration config = TestHelper.getMultiNodeConfig();
      CacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(config);
      CacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(config);
      registerCacheManager(cm1);
      registerCacheManager(cm2);

      hotRodServer1 = TestHelper.startHotRodServer(manager(0));
      hotRodServer2 = TestHelper.startHotRodServer(manager(1));

      manager(0).getCache();
      manager(1).getCache();

      TestingUtil.blockUntilViewReceived(manager(0).getCache(), 2, 10000);
      TestingUtil.blockUntilViewReceived(manager(1).getCache(), 2, 10000);


      manager(0).getCache().put("k","v");
      manager(0).getCache().get("k").equals("v");
      manager(1).getCache().get("k").equals("v");

      log.info("Local replication test passed!");

      //Important: this only connects to one of the two servers!
      remoteCacheManager = new RemoteCacheManager("localhost:" + hotRodServer2.getPort() + ";localhost:" + hotRodServer2.getPort());
      remoteCache = remoteCacheManager.getCache();

      tcpConnectionFactory = (TcpTransportFactory) TestingUtil.extractField(remoteCacheManager, "transportFactory");
   }

   public void testPing() {
      assert tcpConnectionFactory.getServers().size() == 1;
      remoteCache.ping();
      assert tcpConnectionFactory.getServers().size() == 2;
   }

   public void testHashInfoRetrieved() {
      remoteCache.put("k", "v");
   }
}
