package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.transport.netty.DefaultTransportFactory;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
@Test(testName = "client.hotrod.TransportFactoryTest", groups = "functional")
@CleanupAfterTest
public class TransportFactoryTest extends SingleCacheManagerTest {

   private HotRodServer hotrodServer;

   @Override
   protected void setup() throws Exception {
      super.setup();
      hotrodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);
   }

   @Override
   protected void teardown() {
      killServers(hotrodServer);
      super.teardown();
   }

   public void testTransportFactoryProgrammatic() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      TestTransportFactory transportFactory = new TestTransportFactory();
      clientBuilder.addServer().host("localhost").port(hotrodServer.getPort()).transportFactory(transportFactory);
      try (RemoteCacheManager remoteCacheManager = new RemoteCacheManager(clientBuilder.build())) {
         assertEquals(0, transportFactory.socketChannelLatch.getCount());
         assertEquals(0, transportFactory.createEventLoopGroupLatch.getCount());
      }
   }

   public void testTransportFactoryDeclarative() {
      Properties p = new Properties();
      p.setProperty(ConfigurationProperties.TRANSPORT_FACTORY, TestTransportFactory.class.getName());
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("localhost").port(hotrodServer.getPort()).withProperties(p);
      try (RemoteCacheManager remoteCacheManager = new RemoteCacheManager(clientBuilder.build())) {
         Configuration configuration = remoteCacheManager.getConfiguration();
         assertTrue(configuration.transportFactory() instanceof TestTransportFactory);
         TestTransportFactory transportFactory = (TestTransportFactory) configuration.transportFactory();
         assertEquals(0, transportFactory.socketChannelLatch.getCount());
         assertEquals(0, transportFactory.createEventLoopGroupLatch.getCount());
      }
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager();
   }

   public static class TestTransportFactory extends DefaultTransportFactory {
      CountDownLatch socketChannelLatch = new CountDownLatch(1);
      CountDownLatch createEventLoopGroupLatch = new CountDownLatch(1);

      @Override
      public Class<? extends SocketChannel> socketChannelClass() {
         socketChannelLatch.countDown();
         return super.socketChannelClass();
      }

      @Override
      public EventLoopGroup createEventLoopGroup(int maxExecutors, ExecutorService executorService) {
         createEventLoopGroupLatch.countDown();
         return super.createEventLoopGroup(maxExecutors, executorService);
      }
   }
}
