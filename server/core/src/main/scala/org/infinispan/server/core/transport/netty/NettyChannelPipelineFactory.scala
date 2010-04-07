package org.infinispan.server.core.transport.netty

import org.jboss.netty.channel._
import org.infinispan.server.core.ProtocolServer

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
class NettyChannelPipelineFactory(server: ProtocolServer, encoder: ChannelDownstreamHandler)
      extends ChannelPipelineFactory {

   override def getPipeline: ChannelPipeline = {
      val pipeline = Channels.pipeline
      pipeline.addLast("decoder", new DecoderAdapter(server.getDecoder))
      if (encoder != null)
         pipeline.addLast("encoder", encoder)
      return pipeline;
   }

}