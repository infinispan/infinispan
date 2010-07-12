package org.infinispan.client.hotrod.impl.transport.netty;

import org.infinispan.client.hotrod.exceptions.TransportException;

import org.infinispan.io.UnsignedNumeric;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

import static org.infinispan.io.UnsignedNumeric.*;
import static org.jboss.netty.buffer.ChannelBuffers.*;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class HotRodClientEncoder extends OneToOneEncoder {

   @Override
   protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
      if (msg instanceof byte[]) {
         return wrappedBuffer((byte[])msg);
      } else if (msg instanceof Integer) {
         int intMsg = (Integer) msg;
         ChannelBuffer buffer = getBuffer(channel);
         writeUnsignedInt(buffer.toByteBuffer(), intMsg);
         return buffer;
      } else if (msg instanceof Long) {
         ChannelBuffer buffer = getBuffer(channel);
         long longMsg = (Long) msg;
         writeUnsignedLong(buffer.toByteBuffer(), longMsg);
         return buffer;
      } else if (msg instanceof Short) {
         ChannelBuffer buffer = getBuffer(channel);
         short byteMsg =  (Short) msg;
         buffer.writeByte(byteMsg);
         return buffer;
      } else {
         throw new TransportException("Unknown msg type: " + msg.getClass());
      }
   }


   private ChannelBuffer getBuffer(Channel channel) {
      return ChannelBuffers.dynamicBuffer(channel.getConfig().getBufferFactory());
   }
}
