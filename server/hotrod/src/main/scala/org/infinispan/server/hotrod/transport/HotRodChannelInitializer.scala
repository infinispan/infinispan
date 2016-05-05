package org.infinispan.server.hotrod.transport

import java.util.concurrent.Executors

import io.netty.channel.{Channel, ChannelOutboundHandler}
import io.netty.util.concurrent.DefaultThreadFactory
import org.infinispan.server.core.ExecutionHandler
import org.infinispan.server.core.transport.{NettyChannelInitializer, NettyTransport}
import org.infinispan.server.hotrod.logging.{LoggingContextHandler, HotRodAccessLoggingHandler}
import org.infinispan.server.hotrod._

/**
  * HotRod specific channel initializer
  *
  * @author wburns
  * @since 9.0
  */
class HotRodChannelInitializer(val server: HotRodServer, transport: => NettyTransport,
                               val encoder: ChannelOutboundHandler, threadNamePrefix: String)
      extends NettyChannelInitializer(server, transport, encoder) {

   val executionHandler = new ExecutionHandler(Executors.newFixedThreadPool(server.getConfiguration.workerThreads(),
      new DefaultThreadFactory(threadNamePrefix + "ServerHandler")))

   override def initChannel(ch: Channel): Unit = {
      super.initChannel(ch)
      if (server.getConfiguration.authentication().enabled()) {
         ch.pipeline().addLast("authentication", new AuthenticationHandler(server))
      }
      ch.pipeline.addLast("local-handler", new LocalContextHandler(transport))

      // All inbound handlers after this are performed in the executor
      ch.pipeline.addLast("execution-handler", executionHandler)

      ch.pipeline.addLast("handler", new ContextHandler(server, transport))
      ch.pipeline.addLast("exception", new HotRodExceptionHandler)

      // Logging handlers
      ch.pipeline.addBefore("decoder", "logging", new HotRodAccessLoggingHandler)
      ch.pipeline.addAfter("encoder", "logging-context", LoggingContextHandler.getInstance);
   }
}