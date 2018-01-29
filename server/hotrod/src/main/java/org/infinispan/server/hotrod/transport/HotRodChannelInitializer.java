package org.infinispan.server.hotrod.transport;

import java.util.concurrent.ExecutorService;

import org.infinispan.server.core.transport.NettyChannelInitializer;
import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.server.hotrod.AuthenticationHandler;
import org.infinispan.server.hotrod.ContextHandler;
import org.infinispan.server.hotrod.HotRodExceptionHandler;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.LocalContextHandler;
import org.infinispan.server.hotrod.logging.HotRodAccessLoggingHandler;
import org.infinispan.server.hotrod.logging.LoggingContextHandler;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelOutboundHandler;

/**
 * HotRod specific channel initializer
 *
 * @author wburns
 * @since 9.0
 */
public class HotRodChannelInitializer extends NettyChannelInitializer {
   protected final HotRodServer hotRodServer;
   protected final NettyTransport transport;
   protected final ExecutorService executor;

   public HotRodChannelInitializer(HotRodServer hotRodServer, NettyTransport transport, ChannelOutboundHandler encoder,
                                   ChannelInboundHandler decoder, ExecutorService executor) {
      super(hotRodServer, transport, encoder, decoder);
      this.hotRodServer = hotRodServer;
      this.transport = transport;
      this.executor = executor;
   }

   @Override
   public void initializeChannel(Channel ch) throws Exception {
      super.initializeChannel(ch);

      AuthenticationHandler authHandler = hotRodServer.getConfiguration().authentication().enabled() ?
            new AuthenticationHandler(hotRodServer) : null;
      if (authHandler != null) {
         ch.pipeline().addLast("authentication-1", authHandler);
      }
      ch.pipeline().addLast("local-handler", new LocalContextHandler(transport));

      ch.pipeline().addLast("handler", new ContextHandler(hotRodServer, transport, executor));
      ch.pipeline().addLast("exception", new HotRodExceptionHandler());

      // Logging handlers
      if (HotRodAccessLoggingHandler.isEnabled()) {
         ch.pipeline().addBefore("decoder", "logging", new HotRodAccessLoggingHandler());
         ch.pipeline().addAfter("encoder", "logging-context", LoggingContextHandler.getInstance());
      }
   }
}
