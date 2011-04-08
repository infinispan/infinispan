package org.infinispan.server.core.transport

import org.jboss.netty.channel._
import org.infinispan.server.core.ProtocolServer
import org.jboss.netty.handler.timeout.IdleStateHandler
import org.jboss.netty.util.{HashedWheelTimer, Timer}

/**
 * Pipeline factory for Netty based channels. For each pipeline created, a new decoder is created which means that
 * each incoming connection deals with a unique decoder instance. Since the encoder does not maintain any state,
 * a single encoder instance is shared by all incoming connections, if and only if, the protocol mandates an encoder.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
class NettyChannelPipelineFactory(server: ProtocolServer, encoder: ChannelDownstreamHandler,
                                  transport: NettyTransport, idleTimeout: Int)
      extends ChannelPipelineFactory {

   private var timer: Timer = _

   override def getPipeline: ChannelPipeline = {
      val pipeline = Channels.pipeline
      pipeline.addLast("decoder", server.getDecoder)
      if (encoder != null)
         pipeline.addLast("encoder", encoder)
      // Idle timeout logic is disabled with -1 or 0 values
      if (idleTimeout > 0) {
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
