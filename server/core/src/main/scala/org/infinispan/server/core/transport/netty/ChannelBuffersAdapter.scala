package org.infinispan.server.core.transport.netty

import org.infinispan.server.core.transport.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.buffer.{ChannelBuffers => NettyChannelBuffers}
import org.jboss.netty.buffer.{ChannelBuffer => NettyChannelBuffer}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

object ChannelBuffersAdapter extends ChannelBuffers {

   override def wrappedBuffer(array: Array[Byte]*): ChannelBuffer = {
      new ChannelBufferAdapter(NettyChannelBuffers.wrappedBuffer(array : _*));
   }
   
   override def dynamicBuffer(): ChannelBuffer = {
      new ChannelBufferAdapter(NettyChannelBuffers.dynamicBuffer());
   }

}