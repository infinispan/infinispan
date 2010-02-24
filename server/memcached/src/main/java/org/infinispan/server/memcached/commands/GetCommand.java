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
package org.infinispan.server.memcached.commands;

import org.infinispan.Cache;
import org.infinispan.server.core.transport.Channel;
import org.infinispan.server.core.transport.ChannelBuffer;
import org.infinispan.server.core.transport.ChannelBuffers;
import org.infinispan.server.core.transport.ChannelHandlerContext;
import org.infinispan.server.memcached.interceptors.TextProtocolVisitor;

import static org.infinispan.server.memcached.TextProtocolUtil.CRLF;
import static org.infinispan.server.memcached.Reply.VALUE;
import static org.infinispan.server.memcached.Reply.END;

/**
 * GetCommand.
 * 
 * @author Galder Zamarreño
 * @since 4.1
 */
public class GetCommand extends RetrievalCommand {

   GetCommand(Cache<String, Value> cache, CommandType type, RetrievalParameters params) {
      super(cache, type, params);
   }

   @Override
   public Object acceptVisitor(ChannelHandlerContext ctx, TextProtocolVisitor next) throws Throwable {
      return next.visitGet(ctx, this);
   }
   
   @Override
   public Object perform(ChannelHandlerContext ctx) throws Throwable {
      Channel ch = ctx.getChannel();
      ChannelBuffer buffer;
      ChannelBuffers buffers = ctx.getChannelBuffers();
      for (String key : params.keys) {
         Value value = cache.get(key);
         if (value != null) {
            StringBuilder sb = constructValue(key, value);
            buffer = buffers.wrappedBuffer(buffers.wrappedBuffer(sb.toString().getBytes()), buffers.wrappedBuffer(CRLF),
                     buffers.wrappedBuffer(value.getData()), buffers.wrappedBuffer(CRLF));
            ch.write(buffer);
         }
      }
      
      ch.write(buffers.wrappedBuffer(buffers.wrappedBuffer(END.bytes()), buffers.wrappedBuffer(CRLF)));
      return END;
   }

   protected StringBuilder constructValue(String key, Value value) {
      StringBuilder sb = new StringBuilder();
      sb.append(VALUE).append(" ")
         .append(key).append(" ")
         .append(value.getFlags()).append(" ")
         .append(value.getData().length).append(" ");
      return sb;
   }
}
