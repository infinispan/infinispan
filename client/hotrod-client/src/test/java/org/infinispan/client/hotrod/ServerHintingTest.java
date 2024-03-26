package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test to ensure that the client maps segmenst properly when servers are using server hinting and also
 * an additional server with zero capacity configured.
 */
@Test(testName = "client.hotrod.ServerHintingTest", groups = "functional")
public class ServerHintingTest extends MultipleCacheManagersTest {

   HotRodServer hotRodServer1;
   HotRodServer hotRodServer2;
   HotRodServer hotRodServer3;
   HotRodServer hotRodServer4;

   RemoteCache cache2;

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
      killServers(hotRodServer1, hotRodServer2, hotRodServer3, hotRodServer4);
      killRemoteCacheManager(remoteCacheManager);
      hotRodServer1 = null;
      hotRodServer2 = null;
      hotRodServer3 = null;
      hotRodServer4 = null;
      remoteCacheManager = null;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      config = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      config.clustering().hash().numOwners(1);
      for (int i = 0; i < 3; ++i) {
         GlobalConfigurationBuilder gb = GlobalConfigurationBuilder.defaultClusteredBuilder();
         gb.transport().siteId(Integer.toString(i));
         CacheContainer cm = TestCacheManagerFactory.createClusteredCacheManager(gb, config);
         registerCacheManager(cm);
      }

      // Add a zero cap node as well
      GlobalConfigurationBuilder gb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gb.transport().siteId("2");
      gb.zeroCapacityNode(true);
      CacheContainer cm = TestCacheManagerFactory.createClusteredCacheManager(gb, config);
      registerCacheManager(cm);

      waitForClusterToForm();
   }

   @BeforeClass
   @Override
   public void createBeforeClass() throws Throwable {
      super.createBeforeClass(); // Create cache managers
      hotRodServer1 = HotRodClientTestingUtil.startHotRodServer(manager(0));
      hotRodServer2 = HotRodClientTestingUtil.startHotRodServer(manager(1));
      hotRodServer3 = HotRodClientTestingUtil.startHotRodServer(manager(2));
      hotRodServer4 = HotRodClientTestingUtil.startHotRodServer(manager(3));

      //Important: this only connects to one of the three servers!
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotRodServer2.getPort());
      remoteCacheManager = new InternalRemoteCacheManager(clientBuilder.build());
      cache2 = remoteCacheManager.getCache();

      channelFactory = ((InternalRemoteCacheManager) remoteCacheManager).getChannelFactory();
   }

   @Test
   public void testClientSegmentsMapProperly() {

      // Send a command just in case topology wasn't updated
      cache2.put("foo", "bar");

      CacheTopologyInfo cti = cache2.getCacheTopologyInfo();;
      assertEquals(3, cti.getSegmentsPerServer().entrySet().size());
      for (Map.Entry<SocketAddress, Set<Integer>> entry : cti.getSegmentsPerServer().entrySet()) {
         IntSet serverPrimarySegments;
         InetSocketAddress clientAddress = (InetSocketAddress) entry.getKey();
         if (hotRodServer1.getAddress().getPort() ==  clientAddress.getPort()) {
            serverPrimarySegments = cache(0).getAdvancedCache().getDistributionManager().getCacheTopology().getLocalPrimarySegments();
         } else if (hotRodServer2.getAddress().getPort() ==  clientAddress.getPort()) {
            serverPrimarySegments = cache(1).getAdvancedCache().getDistributionManager().getCacheTopology().getLocalPrimarySegments();
         } else {
            assert hotRodServer3.getAddress().getPort() ==  clientAddress.getPort();
            serverPrimarySegments = cache(2).getAdvancedCache().getDistributionManager().getCacheTopology().getLocalPrimarySegments();
         }
         Set<Integer> clientPrimarySegments = entry.getValue();

         assertEquals("Segments should be same server was : " + serverPrimarySegments + " and client was: " + clientPrimarySegments,
               clientPrimarySegments.size(), serverPrimarySegments.size());

         serverPrimarySegments.forEach((Consumer<? super Integer>) segment ->
            assertTrue("Wasn't primary owner of segment " + segment, clientPrimarySegments.contains(segment)));
      }
   }
}
