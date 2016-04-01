package org.infinispan.server.hotrod.transport

import io.netty.channel.{Channel, ChannelInitializer}
import io.netty.handler.codec.FixedLengthFrameDecoder
import io.netty.handler.timeout.IdleStateHandler
import org.infinispan.server.core.ProtocolServer

/**
 * A channel pipeline factory for testing that will inject a fixed length frame encoder of 2 bytes before a
 * channel handler named <b>decoder</b>
 *
 * @author William Burns
 * @since 9.0
 */
trait SingleByteFrameDecoderChannelInitializer extends ChannelInitializer[Channel] {
   abstract override def initChannel(ch: Channel): Unit = {
      super.initChannel(ch)
      val pipeline = ch.pipeline
      pipeline.addBefore("decoder", "twoframe", new FixedLengthFrameDecoder(1))
   }
}