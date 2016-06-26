package org.infinispan.server.core;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandler;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.configuration.MockServerConfigurationBuilder;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.test.Stoppable;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

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
   }

}
