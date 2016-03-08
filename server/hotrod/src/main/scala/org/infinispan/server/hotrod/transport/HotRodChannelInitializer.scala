package org.infinispan.server.hotrod.transport

import io.netty.channel.{Channel, ChannelOutboundHandler}
import io.netty.util.concurrent.{DefaultEventExecutorGroup, DefaultThreadFactory}
import org.infinispan.server.core.transport.{NettyChannelInitializer, NettyTransport}
import org.infinispan.server.hotrod.logging.{LoggingContextHandler, HotRodAccessLoggingHandler}
import org.infinispan.server.hotrod.{HotRodExceptionHandler, AuthenticationHandler, ContextHandler, HotRodServer}

/**
  * HotRod specific channel initializer
  *
  * @author wburns
  * @since 9.0
  */
class HotRodChannelInitializer(val server: HotRodServer, transport: => NettyTransport,
                               val encoder: ChannelOutboundHandler, threadNamePrefix: String)
      extends NettyChannelInitializer(server, transport, encoder) {

   override def initChannel(ch: Channel): Unit = {
      super.initChannel(ch)
      // Any inbound handler after this point should really be using this executionGroup
      val executionGroup = new DefaultEventExecutorGroup(transport.configuration.workerThreads,
         new DefaultThreadFactory(threadNamePrefix + "ServerHandler"))
      if (server.getConfiguration.authentication().enabled()) {
         ch.pipeline().addLast(executionGroup, "authentication", new AuthenticationHandler(server))
      }
      ch.pipeline.addLast(executionGroup, "handler", new ContextHandler(server, transport))
      ch.pipeline.addLast(executionGroup, "exception", new HotRodExceptionHandler)

      // Logging handlers
      ch.pipeline.addBefore("decoder", "logging", new HotRodAccessLoggingHandler)
      ch.pipeline.addAfter("encoder", "logging-context", LoggingContextHandler.getInstance);
   }
}