package org.infinispan.client.hotrod.impl.transport.netty;

import org.infinispan.client.hotrod.exceptions.TransportException;

import org.infinispan.io.UnsignedNumeric;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import java.io.IOException;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class HotRodClientDecoder extends FrameDecoder {

   private final ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();

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
      try {
         return UnsignedNumeric.readUnsignedLong(buffer.toByteBuffer());
      } catch (IOException e) {
         throw new RuntimeException("Unable to read unsigned long", e);
      }
   }

   public int readVInt() {
      try {
         return UnsignedNumeric.readUnsignedInt(buffer.toByteBuffer());
      } catch (IOException e) {
         throw new RuntimeException("Unable to read unsigned int", e);
      }
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
