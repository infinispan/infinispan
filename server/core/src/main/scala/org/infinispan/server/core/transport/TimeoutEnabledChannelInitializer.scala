package org.infinispan.server.core.transport

import org.infinispan.server.core.ProtocolServer
import org.infinispan.server.core.configuration.SslConfiguration
import io.netty.channel.{Channel, ChannelOutboundHandler}
import io.netty.handler.timeout.IdleStateHandler

/**
 * A channel pipeline factory for environments where idle timeout is enabled.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
class TimeoutEnabledChannelInitializer(server: ProtocolServer,
                                           encoder: ChannelOutboundHandler)
      extends NettyChannelInitializer(server, encoder) {

   override def initChannel(ch: Channel): Unit = {
      super.initChannel(ch)
      val pipeline = ch.pipeline
      pipeline.addLast("idleHandler", new IdleStateHandler(server.getConfiguration.idleTimeout, 0, 0))
      pipeline.addLast("idleHandlerProvider", new IdleStateHandlerProvider)
   }
}