package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;

import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.CacheContainer;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

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
   private ChannelFactory channelFactory;
   private ConfigurationBuilder config;

   @AfterMethod
   @Override
   protected void clearContent() {
   }

   @AfterClass
   @Override
   protected void destroy() {
      super.destroy();
      killServers(hotRodServer1, hotRodServer2);
      killRemoteCacheManager(remoteCacheManager);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      config = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(getCacheMode(), false));
      CacheContainer cm1 = TestCacheManagerFactory.createClusteredCacheManager(config);
      CacheContainer cm2 = TestCacheManagerFactory.createClusteredCacheManager(config);
      registerCacheManager(cm1);
      registerCacheManager(cm2);
      waitForClusterToForm();
   }

   @BeforeClass
   @Override
   public void createBeforeClass() throws Throwable {
      super.createBeforeClass(); // Create cache managers
      hotRodServer1 = HotRodClientTestingUtil.startHotRodServer(manager(0));
      hotRodServer2 = HotRodClientTestingUtil.startHotRodServer(manager(1));

      //Important: this only connects to one of the two servers!
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder.addServer().host("localhost").port(hotRodServer2.getPort());
      remoteCacheManager = new InternalRemoteCacheManager(clientBuilder.build());
      remoteCache = remoteCacheManager.getCache();

      channelFactory = ((InternalRemoteCacheManager) remoteCacheManager).getChannelFactory();
   }

   protected CacheMode getCacheMode() {
      return CacheMode.REPL_SYNC;
   }

   public void testTwoMembers() {
      InetSocketAddress server1Address = InetSocketAddress.createUnresolved(hotRodServer1.getHost(), hotRodServer1.getPort());
      expectTopologyChange(server1Address, true);
      assertEquals(2, channelFactory.getServers().size());
   }

   @Test(dependsOnMethods = "testTwoMembers")
   public void testAddNewServer() {
      CacheContainer cm3 = TestCacheManagerFactory.createClusteredCacheManager(config);
      registerCacheManager(cm3);
      hotRodServer3 = HotRodClientTestingUtil.startHotRodServer(manager(2));
      manager(2).getCache();

      waitForClusterToForm();

      try {
         expectTopologyChange(InetSocketAddress.createUnresolved(hotRodServer3.getHost(), hotRodServer3.getPort()), true);
         assertEquals(3, channelFactory.getServers().size());
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
      log.trace("Just stopped server 2");

      waitForServerToDie(2);

      InetSocketAddress server3Address = InetSocketAddress.createUnresolved(hotRodServer3.getHost(), hotRodServer3.getPort());

      try {
         expectTopologyChange(server3Address, false);
         assertEquals(2, channelFactory.getServers().size());
      } finally {
         log.info("Members are: " + manager(0).getCache().getAdvancedCache().getRpcManager().getTransport().getMembers());
         log.info("Members are: " + manager(1).getCache().getAdvancedCache().getRpcManager().getTransport().getMembers());
         if (manager(2).getStatus() != ComponentStatus.RUNNING)
            log.info("Members are: 0");
         else
            log.info("Members are: " + manager(2).getCache().getAdvancedCache().getRpcManager().getTransport().getMembers());
      }
   }

   private void expectTopologyChange(InetSocketAddress server1Address, boolean added) {
      for (int i = 0; i < 10; i++) {
         remoteCache.put("k" + i, "v" + i);
         if (added == channelFactory.getServers().contains(server1Address)) break;
      }
      Collection<SocketAddress> addresses = channelFactory.getServers();
      assertEquals(server1Address + " not found in " + addresses, added, addresses.contains(server1Address));
   }

   protected void waitForServerToDie(int memberCount) {
      TestingUtil.blockUntilViewReceived(manager(0).getCache(), memberCount, 30000, false);
   }
}
