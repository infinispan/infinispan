/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat, Inc. and/or its affiliates, and
 * individual contributors as indicated by the @author tags. See the
 * copyright.txt file in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.server.memcached;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.Cache;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.handler.codec.replay.ReplayingDecoder;
import static org.infinispan.server.memcached.TextProtocolUtil.*;
import static org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer;

/**
 * TextCommandDecoder.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class TextCommandDecoder extends ReplayingDecoder<TextCommandDecoder.State> {
   private static final Log log = LogFactory.getLog(TextCommandDecoder.class);
   
   private final CommandFactory factory;
   private volatile Command command;
//   private final AtomicBoolean corrupted = new AtomicBoolean();

   protected enum State {
      READ_COMMAND, READ_UNSTRUCTURED_DATA;
   }

   TextCommandDecoder(Cache cache, BlockingQueue<DeleteDelayedEntry> queue) {
      super(State.READ_COMMAND, true);
      factory = new CommandFactory(cache, queue);
   }

   @Override
   protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer, State state) throws Exception {
      switch (state) {
         case READ_COMMAND:
            String line = readLine(buffer);
//            try {
               command = factory.createCommand(line);
//               corrupted.set(false);
//            } catch (IOException ioe) {
//               if (corrupted.get())
//                  log.debug("Channel is corrupted and we're reading garbage, ignore read until we find a good command again");
//               else
//                  throw ioe;
//            }
            
            if (command.getType().isStorage())
               checkpoint(State.READ_UNSTRUCTURED_DATA);
            else
               return command;
            break;
         case READ_UNSTRUCTURED_DATA:
            StorageCommand storageCmd = (StorageCommand) command;
            byte[] data= new byte[storageCmd.params.bytes];
            buffer.readBytes(data, 0, data.length);
            byte next = buffer.readByte();
            if (next == CR) {
               next = buffer.readByte();
               if (next == LF) {
                  try {
                     return reset(storageCmd.setData(data));
                  } catch (IOException ioe) {
                     checkpoint(State.READ_COMMAND);
                     throw ioe;
                  }
               } else {
                  throw new StreamCorruptedException("Expecting \r\n after data block");
               }
            } else {
               throw new StreamCorruptedException("Expecting \r\n after data block");
            }
      }
      return null;
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
//      corrupted.compareAndSet(false, true);
      Throwable t = e.getCause();
      log.error("Unexpected exception", t);
      Channel ch = ctx.getChannel();
      if (t instanceof UnknownCommandException) {
         ch.write(wrappedBuffer(wrappedBuffer(Reply.ERROR.bytes()), wrappedBuffer(CRLF)));
      } else if (t instanceof IOException) {
         StringBuilder sb = new StringBuilder();
         sb.append(Reply.CLIENT_ERROR).append(' ').append(t);
         ch.write(wrappedBuffer(wrappedBuffer(sb.toString().getBytes()), wrappedBuffer(CRLF)));
      } else {
         StringBuilder sb = new StringBuilder();
         sb.append(Reply.SERVER_ERROR).append(' ').append(t);
         ch.write(wrappedBuffer(wrappedBuffer(sb.toString().getBytes()), wrappedBuffer(CRLF)));
      }
   }

   private Object reset(Command c) {
      this.command = null;
      checkpoint(State.READ_COMMAND);
      return c;
  }

   private String readLine(ChannelBuffer buffer) {
      StringBuilder sb = new StringBuilder(64);
      int lineLength = 0;
      while (true) {
         byte next = buffer.readByte();
         if (next == CR) {
            next = buffer.readByte();
            if (next == LF) {
               return sb.toString();
            }
         } else if (next == LF) {
            return sb.toString();
         } else {
            lineLength++;
            sb.append((char) next);
         }
      }
   }

   // private String readLine(ChannelBuffer buffer) {
   // int minFrameLength = Integer.MAX_VALUE;
   //
   // ChannelBuffer minDelim = null;
   // int frameLength = indexOf(buffer, CR);
   // if (frameLength >= 0 && frameLength < minFrameLength) {
   // minFrameLength = frameLength;
   // minDelim = CR;
   // }
   //
   // if (minDelim != null) {
   // int minDelimLength = minDelim.capacity();
   // ChannelBuffer frame = buffer.readBytes(minFrameLength);
   // buffer.skipBytes(minDelimLength);
   // return frame.toString(Charset.defaultCharset().name());
   // } else {
   // return null;
   // }
   // }
   //
   // /**
   // * Returns the number of bytes between the readerIndex of the haystack and
   // * the first needle found in the haystack. -1 is returned if no needle is
   // * found in the haystack.
   // */
   // private static int indexOf(ChannelBuffer haystack, ChannelBuffer needle) {
   // for (int i = haystack.readerIndex(); i < haystack.writerIndex(); i ++) {
   // int haystackIndex = i;
   // int needleIndex;
   // for (needleIndex = 0; needleIndex < needle.capacity(); needleIndex ++) {
   // if (haystack.getByte(haystackIndex) != needle.getByte(needleIndex)) {
   // break;
   // } else {
   // haystackIndex ++;
   // if (haystackIndex == haystack.writerIndex() &&
   // needleIndex != needle.capacity() - 1) {
   // return -1;
   // }
   // }
   // }
   //
   // if (needleIndex == needle.capacity()) {
   // // Found the needle from the haystack!
   // return i - haystack.readerIndex();
   // }
   // }
   // return -1;
   // }
}
