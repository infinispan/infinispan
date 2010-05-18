package org.infinispan.client.hotrod.impl.transport.netty;

import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.transport.VHelper;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class HotrodClientDecoder extends FrameDecoder {

   private final ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
   private final InputStreamAdapter isa = new InputStreamAdapter(buffer);

   @Override
   protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
      synchronized (this.buffer) {
         this.buffer.writeBytes(buffer);
         if (this.buffer.readableBytes() > 0) {
            this.buffer.notify();
         }
      }
      return null;
   }

   public long readVLong() {
      return VHelper.readVLong(isa);
   }

   public int readVInt() {
      return VHelper.readVInt(isa);
   }

   public void fillBuffer(byte[] bufferToFill) {
      synchronized (buffer) {
         if (buffer.readableBytes() < bufferToFill.length) {
            try {
               buffer.wait();
            } catch (InterruptedException e) {
               throw new TransportException(e);
            }
         }
         buffer.readBytes(bufferToFill);
      }
   }

   public short readByte() {
      synchronized (buffer) {
         if (!buffer.readable()) {
            try {
               buffer.wait();
            } catch (InterruptedException e) {
               throw new TransportException(e);
            }
         }
         return buffer.readUnsignedByte();
      }
   }
}
