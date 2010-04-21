package org.infinispan.server.core.transport.netty

import org.jboss.netty.channel._
import org.infinispan.server.core.ProtocolServer
import org.jboss.netty.handler.timeout.IdleStateHandler
import org.infinispan.server.core.transport.IdleStateHandlerProvider
import org.jboss.netty.util.{HashedWheelTimer, Timer}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
class NettyChannelPipelineFactory(server: ProtocolServer, encoder: ChannelDownstreamHandler,
                                  transport: NettyTransport, idleTimeout: Int)
      extends ChannelPipelineFactory {

   private var timer: Timer = _

   override def getPipeline: ChannelPipeline = {
      val pipeline = Channels.pipeline
      pipeline.addLast("decoder", new DecoderAdapter(server.getDecoder, transport))
      if (encoder != null)
         pipeline.addLast("encoder", encoder)
      if (idleTimeout != 0) {
         timer = new HashedWheelTimer
         pipeline.addLast("idleHandler", new IdleStateHandler(timer, idleTimeout, 0, 0))
         pipeline.addLast("idleHandlerProvider", new IdleStateHandlerProvider)
      }
      return pipeline;
   }

   def stop {
      if (timer != null) timer.stop
   }
}
