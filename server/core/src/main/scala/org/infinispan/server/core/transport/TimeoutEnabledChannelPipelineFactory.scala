package org.infinispan.server.core.transport

import org.jboss.netty.util.HashedWheelTimer
import org.jboss.netty.handler.timeout.IdleStateHandler
import org.infinispan.server.core.ProtocolServer
import org.jboss.netty.channel.{ChannelDownstreamHandler, Channels, ChannelPipeline}
import org.jboss.netty.handler.ssl.SslHandler
import org.infinispan.server.core.configuration.SslConfiguration

/**
 * A channel pipeline factory for environments where idle timeout is enabled.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
class TimeoutEnabledChannelPipelineFactory(server: ProtocolServer,
                                           encoder: ChannelDownstreamHandler)
      extends NettyChannelPipelineFactory(server, encoder) {

   import TimeoutEnabledChannelPipelineFactory._

   override def getPipeline: ChannelPipeline = {
      val pipeline = super.getPipeline

      pipeline.addLast("idleHandler", new IdleStateHandler(timer, server.getConfiguration.idleTimeout, 0, 0))
      pipeline.addLast("idleHandlerProvider", new IdleStateHandlerProvider)
      return pipeline;
   }

   override def stop {
      timer.stop()
   }

}

object TimeoutEnabledChannelPipelineFactory {

   lazy val timer = new HashedWheelTimer

}