package org.infinispan.server.core.transport

import org.infinispan.server.core.ProtocolServer
import org.infinispan.server.core.configuration.SslConfiguration
import io.netty.channel.{ChannelInitializer, Channel}
import io.netty.handler.timeout.IdleStateHandler

/**
 * A channel pipeline factory for environments where idle timeout is enabled.  This is a trait, useful to extend
 * by an implementation channel initializer.
 *
 * @author Galder Zamarreño
 * @author William Burns
 * @since 5.1
 */
trait TimeoutEnabledChannelInitializer extends ChannelInitializer[Channel] {
   val server: ProtocolServer

   abstract override def initChannel(ch: Channel): Unit = {
      super.initChannel(ch)
      val pipeline = ch.pipeline
      pipeline.addLast("idleHandler", new IdleStateHandler(server.getConfiguration.idleTimeout, 0, 0))
      pipeline.addLast("idleHandlerProvider", new IdleStateHandlerProvider)
   }
}