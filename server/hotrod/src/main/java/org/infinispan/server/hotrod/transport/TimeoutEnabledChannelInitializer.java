package org.infinispan.server.hotrod.transport;

import org.infinispan.server.core.ProtocolServer;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.transport.IdleStateHandlerProvider;
import org.infinispan.server.core.transport.NettyInitializer;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.timeout.IdleStateHandler;

/**
 * A channel pipeline factory for environments where idle timeout is enabled.  This is a trait, useful to extend by an
 * implementation channel initializer.
 *
 * @author Galder Zamarreño
 * @author William Burns
 * @since 5.1
 */
public class TimeoutEnabledChannelInitializer<C extends ProtocolServerConfiguration> implements NettyInitializer {
   private final ProtocolServer<C> hotRodServer;

   public TimeoutEnabledChannelInitializer(ProtocolServer<C> hotRodServer) {
      this.hotRodServer = hotRodServer;
   }

   @Override
   public void initializeChannel(Channel ch) throws Exception {
      ChannelPipeline pipeline = ch.pipeline();
      pipeline.addLast("idleHandler", new IdleStateHandler(hotRodServer.getConfiguration().idleTimeout(), 0, 0));
      pipeline.addLast("idleHandlerProvider", new IdleStateHandlerProvider());
   }
}
