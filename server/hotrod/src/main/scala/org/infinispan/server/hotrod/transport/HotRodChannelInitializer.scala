package org.infinispan.server.hotrod.transport

import java.util.concurrent.Executors

import io.netty.channel.{Channel, ChannelOutboundHandler}
import io.netty.util.concurrent.DefaultThreadFactory
import org.infinispan.commons.logging.LogFactory
import org.infinispan.server.core.transport.{NettyChannelInitializer, NettyTransport}
import org.infinispan.server.hotrod._
import org.infinispan.server.hotrod.logging.{HotRodAccessLoggingHandler, JavaLog, LoggingContextHandler}

/**
  * HotRod specific channel initializer
  *
  * @author wburns
  * @since 9.0
  */
class HotRodChannelInitializer(val server: HotRodServer, transport: => NettyTransport,
                               val encoder: ChannelOutboundHandler, threadNamePrefix: String)
      extends NettyChannelInitializer(server, transport, encoder) {

   val executor = Executors.newFixedThreadPool(server.getConfiguration.workerThreads(),
      new DefaultThreadFactory(threadNamePrefix + "ServerHandler"))
//   val executor = new WithinThreadExecutor

   override def initChannel(ch: Channel): Unit = {
      super.initChannel(ch)
      val authHandler = if (server.getConfiguration.authentication().enabled()) new AuthenticationHandler(server) else null
      if (authHandler != null) {
         // TODO: We need to move this to the executor thread as well
         ch.pipeline().addLast("authentication-1", authHandler)
      }
      ch.pipeline.addLast("local-handler", new LocalContextHandler(transport))

      ch.pipeline.addLast("handler", new ContextHandler(server, transport, executor))
      ch.pipeline.addLast("exception", new HotRodExceptionHandler)

      // Logging handlers
      if (LogFactory.getLog(classOf[HotRodAccessLoggingHandler], classOf[JavaLog]).isTraceEnabled) {
         ch.pipeline.addBefore("decoder", "logging", new HotRodAccessLoggingHandler)
         ch.pipeline.addAfter("encoder", "logging-context", LoggingContextHandler.getInstance)
      }
   }
}
