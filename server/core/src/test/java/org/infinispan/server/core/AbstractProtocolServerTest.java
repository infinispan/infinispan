package org.infinispan.server.core;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.server.core.configuration.MockServerConfigurationBuilder;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.Assert;
import org.testng.annotations.Test;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandler;

/**
 * Abstract protocol server test.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = "functional", testName = "server.core.AbstractProtocolServerTest")
public class AbstractProtocolServerTest extends AbstractInfinispanTest {

   public void testValidateNegativeWorkerThreads() {
      MockServerConfigurationBuilder b = new MockServerConfigurationBuilder();
      b.workerThreads(-1);
      expectIllegalArgument(b, new MockProtocolServer());
   }

   public void testValidateNegativeIdleTimeout() {
      MockServerConfigurationBuilder b = new MockServerConfigurationBuilder();
      b.idleTimeout(-2);
      expectIllegalArgument(b, new MockProtocolServer());
   }

   public void testValidateNegativeSendBufSize() {
      MockServerConfigurationBuilder b = new MockServerConfigurationBuilder();
      b.sendBufSize(-1);
      expectIllegalArgument(b, new MockProtocolServer());
   }

   public void testValidateNegativeRecvBufSize() {
      MockServerConfigurationBuilder b = new MockServerConfigurationBuilder();
      b.recvBufSize(-1);
      expectIllegalArgument(b, new MockProtocolServer());
   }

   public void testStartingWithoutTransport() {
      MockServerConfigurationBuilder b = new MockServerConfigurationBuilder();
      b.startTransport(false);
      AbstractProtocolServer server = new MockProtocolServer();
      DefaultCacheManager manager = new DefaultCacheManager(new ConfigurationBuilder().build());
      try {
         server.start(b.build(), manager);
         Assert.assertFalse(server.isTransportEnabled());
      } finally {
         server.stop();
         manager.stop();
      }
   }

   private void expectIllegalArgument(MockServerConfigurationBuilder builder, MockProtocolServer server) {
      try {
//         Stoppable.useCacheManager(TestCacheManagerFactory.createCacheManager()) { cm =>
//            server.start(builder.build(), cm)
//         }
      } catch (IllegalArgumentException e) {
//         case i: IllegalArgumentException => // expected
      } finally {
         server.stop();
      }
   }

   class MockProtocolServer extends AbstractProtocolServer {
      protected MockProtocolServer() {
         super(null);
      }

      @Override
      public ChannelOutboundHandler getEncoder() {
         return null;
      }

      @Override
      public ChannelInboundHandler getDecoder() {
         return null;
      }

      @Override
      public ProtocolServerConfiguration getConfiguration() {
         return configuration;
      }

      @Override
      public ChannelInitializer<Channel> getInitializer() {
         return null;
      }

      @Override
      public int getWorkerThreads() {
         return configuration.workerThreads();
      }
   }

}
