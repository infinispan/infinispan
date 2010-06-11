package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.config.Configuration;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.CacheContainer;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;

import static org.testng.AssertJUnit.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "client.hotrod.ReplTopologyChangeTest", groups = "functional")
public class ReplTopologyChangeTest extends MultipleCacheManagersTest {

   HotRodServer hotRodServer1;
   HotRodServer hotRodServer2;
   HotRodServer hotRodServer3 ;

   RemoteCache remoteCache;
   private RemoteCacheManager remoteCacheManager;
   private TcpTransportFactory tcpConnectionFactory;
   private Configuration config;

   @Override
   protected void assertSupportedConfig() {
   }

   @AfterMethod
   @Override
   protected void clearContent() throws Throwable {
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      config = getDefaultClusteredConfig(getCacheMode());
      CacheContainer cm1 = TestCacheManagerFactory.createClusteredCacheManager(config);
      CacheContainer cm2 = TestCacheManagerFactory.createClusteredCacheManager(config);
      registerCacheManager(cm1);
      registerCacheManager(cm2);

      hotRodServer1 = TestHelper.startHotRodServer(manager(0));
      hotRodServer2 = TestHelper.startHotRodServer(manager(1));

      manager(0).getCache();
      manager(1).getCache();

      TestingUtil.blockUntilViewReceived(manager(0).getCache(), 2, 10000);
      TestingUtil.blockUntilCacheStatusAchieved(manager(0).getCache(), ComponentStatus.RUNNING, 10000);
      TestingUtil.blockUntilCacheStatusAchieved(manager(1).getCache(), ComponentStatus.RUNNING, 10000);

      manager(0).getCache().put("k", "v");
      manager(0).getCache().get("k").equals("v");
      manager(1).getCache().get("k").equals("v");

      log.info("Local replication test passed!");

      //Important: this only connects to one of the two servers!
      remoteCacheManager = new RemoteCacheManager("localhost", hotRodServer2.getPort());
      remoteCache = remoteCacheManager.getCache();

      tcpConnectionFactory = (TcpTransportFactory) TestingUtil.extractField(remoteCacheManager, "transportFactory");
   }

   protected Configuration.CacheMode getCacheMode() {
      return Configuration.CacheMode.REPL_SYNC;
   }


   public void testTwoMembers() {
      InetSocketAddress server1Address = new InetSocketAddress("localhost", hotRodServer1.getPort());
      expectTopologyChange(server1Address, true);
      assertEquals(2, tcpConnectionFactory.getServers().size());
   }

   @Test(dependsOnMethods = "testTwoMembers")
   public void testAddNewServer() {
      CacheContainer cm3 = TestCacheManagerFactory.createClusteredCacheManager(config);
      registerCacheManager(cm3);
      hotRodServer3 = TestHelper.startHotRodServer(manager(2));
      manager(2).getCache();

      TestingUtil.blockUntilViewsReceived(10000, true, manager(0), manager(1), manager(2));
      TestingUtil.blockUntilCacheStatusAchieved(manager(0).getCache(), ComponentStatus.RUNNING, 10000);
      TestingUtil.blockUntilCacheStatusAchieved(manager(1).getCache(), ComponentStatus.RUNNING, 10000);
      TestingUtil.blockUntilCacheStatusAchieved(manager(2).getCache(), ComponentStatus.RUNNING, 10000);

      try {
         expectTopologyChange(new InetSocketAddress("localhost", hotRodServer3.getPort()), true);
         assertEquals(3, tcpConnectionFactory.getServers().size());
      } finally {
         log.info("Members are: " + manager(0).getCache().getAdvancedCache().getRpcManager().getTransport().getMembers());
         log.info("Members are: " + manager(1).getCache().getAdvancedCache().getRpcManager().getTransport().getMembers());
         log.info("Members are: " + manager(2).getCache().getAdvancedCache().getRpcManager().getTransport().getMembers());
      }
   }

   @Test(dependsOnMethods = "testAddNewServer")
   public void testDropServer() {
      hotRodServer3.stop();
      manager(2).stop();
      TestingUtil.blockUntilViewsReceived(10000, true, manager(0), manager(1));
      TestingUtil.blockUntilCacheStatusAchieved(manager(0).getCache(), ComponentStatus.RUNNING, 10000);
      TestingUtil.blockUntilCacheStatusAchieved(manager(1).getCache(), ComponentStatus.RUNNING, 10000);

      InetSocketAddress server3Address = new InetSocketAddress("localhost", hotRodServer3.getPort());
      try {
         expectTopologyChange(server3Address, false);
         assertEquals(2, tcpConnectionFactory.getServers().size());
      } finally {
         log.info("Members are: " + manager(0).getCache().getAdvancedCache().getRpcManager().getTransport().getMembers());
         log.info("Members are: " + manager(1).getCache().getAdvancedCache().getRpcManager().getTransport().getMembers());
         log.info("Members are: " + manager(2).getCache().getAdvancedCache().getRpcManager().getTransport().getMembers());
      }
   }

   private void expectTopologyChange(InetSocketAddress server1Address, boolean added) {
      for (int i = 0; i < 10; i++) {
         try {
            remoteCache.put("k" + i, "v" + i);
         } catch (Exception e) {
            if (added) {
               throw new IllegalStateException(e);
            } //else it is acceptable, as the transport hasn't changed
         }
         if (added == tcpConnectionFactory.getServers().contains(server1Address)) break;
      }
      assertEquals(server1Address + " not found", added, tcpConnectionFactory.getServers().contains(server1Address));
   }
}
