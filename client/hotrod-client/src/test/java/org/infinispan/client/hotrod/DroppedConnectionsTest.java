package org.infinispan.client.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotSame;

import java.net.InetSocketAddress;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.client.hotrod.test.NoopChannelOperation;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import io.netty.channel.Channel;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "client.hotrod.DroppedConnectionsTest", groups = "functional")
public class DroppedConnectionsTest extends SingleCacheManagerTest {
   private HotRodServer hotRodServer;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(
            hotRodCacheConfiguration(getDefaultStandaloneCacheConfig(false)));
      hotRodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);
      return cacheManager;
   }

   @AfterClass
   @Override
   protected void teardown() {
      super.teardown();
      HotRodClientTestingUtil.killServers(hotRodServer);
      hotRodServer = null;
   }

   public void testClosedConnection() throws Exception {

         ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
         clientBuilder
               .connectionPool()
               .minIdle(1)
               .maxActive(2)
               .addServer().host(hotRodServer.getHost()).port(hotRodServer.getPort());
      RemoteCacheManager remoteCacheManager = null;
      try {
         remoteCacheManager = new InternalRemoteCacheManager(clientBuilder.build());
         RemoteCache<String, String> rc = remoteCacheManager.getCache();
         ChannelFactory channelFactory = ((InternalRemoteCacheManager) remoteCacheManager).getChannelFactory();

         rc.put("k", "v"); //make sure a connection is created

         InetSocketAddress address = InetSocketAddress.createUnresolved("127.0.0.1", hotRodServer.getPort());

         assertEquals(0, channelFactory.getNumActive(address));
         assertEquals(1, channelFactory.getNumIdle(address));

         Channel channel = channelFactory.fetchChannelAndInvoke(address, new NoopChannelOperation()).join();
         channelFactory.releaseChannel(channel);//now we have a reference to the single connection in pool

         channel.close().sync();

         assertEquals("v", rc.get("k"));

         assertEquals(0, channelFactory.getNumActive(address));
         assertEquals(1, channelFactory.getNumIdle(address));

         Channel channel2 = channelFactory.fetchChannelAndInvoke(address, new NoopChannelOperation()).join();

         assertNotSame(channel.id(), channel2.id());
      } finally {
         HotRodClientTestingUtil.killRemoteCacheManager(remoteCacheManager);
      }
   }

}
