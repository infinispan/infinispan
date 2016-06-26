package org.infinispan.server.hotrod.transport

import io.netty.channel.{Channel, ChannelInitializer}
import io.netty.handler.timeout.IdleStateHandler
import org.infinispan.server.core.ProtocolServer
import org.infinispan.server.core.configuration.ProtocolServerConfiguration
import org.infinispan.server.core.transport.IdleStateHandlerProvider

/**
 * A channel pipeline factory for environments where idle timeout is enabled.  This is a trait, useful to extend
 * by an implementation channel initializer.
 *
 * @author Galder Zamarreño
 * @author William Burns
 * @since 5.1
 */
trait TimeoutEnabledChannelInitializer[C <: ProtocolServerConfiguration] extends ChannelInitializer[Channel] {
   val hotRodServer: ProtocolServer[C]

   abstract override def initChannel(ch: Channel): Unit = {
      super.initChannel(ch)
      val pipeline = ch.pipeline
      pipeline.addLast("idleHandler", new IdleStateHandler(hotRodServer.getConfiguration.idleTimeout, 0, 0))
      pipeline.addLast("idleHandlerProvider", new IdleStateHandlerProvider)
   }
}