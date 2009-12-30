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
package org.infinispan.server.core.netty.memcached;

import static org.infinispan.server.memcached.TextProtocolUtil.CR;
import static org.infinispan.server.memcached.TextProtocolUtil.CRLF;
import static org.infinispan.server.memcached.TextProtocolUtil.LF;
import static org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.util.concurrent.ScheduledExecutorService;

import org.infinispan.Cache;
import org.infinispan.server.core.Command;
import org.infinispan.server.core.InterceptorChain;
import org.infinispan.server.memcached.Reply;
import org.infinispan.server.memcached.UnknownCommandException;
import org.infinispan.server.memcached.commands.CommandFactory;
import org.infinispan.server.memcached.commands.StorageCommand;
import org.infinispan.server.memcached.commands.TextCommand;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.handler.codec.replay.ReplayingDecoder;

/**
 * NettyMemcachedDecoder.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class NettyMemcachedDecoder extends ReplayingDecoder<NettyMemcachedDecoder.State> {
   private static final Log log = LogFactory.getLog(NettyMemcachedDecoder.class);
   
   private final CommandFactory factory;
   private volatile TextCommand command;

   protected enum State {
      READ_COMMAND, READ_UNSTRUCTURED_DATA;
   }

   public NettyMemcachedDecoder(Cache cache, InterceptorChain chain, ScheduledExecutorService scheduler) {
      super(State.READ_COMMAND, true);
      factory = new CommandFactory(cache, chain, scheduler);
   }

   @Override
   protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer, State state) throws Exception {
      switch (state) {
         case READ_COMMAND:
            command = factory.createCommand(readLine(buffer));
            if (command.getType().isStorage())
               checkpoint(State.READ_UNSTRUCTURED_DATA);
            else
               return command;
            break;
         case READ_UNSTRUCTURED_DATA:
            StorageCommand storageCmd = (StorageCommand) command;
            byte[] data= new byte[storageCmd.getParams().getBytes()];
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

}
