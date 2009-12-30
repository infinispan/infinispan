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

import static org.infinispan.server.memcached.TextProtocolUtil.CRLF;

import org.infinispan.Cache;
import org.infinispan.server.core.Channel;
import org.infinispan.server.core.ChannelBuffers;
import org.infinispan.server.core.ChannelHandlerContext;
import org.infinispan.server.memcached.Reply;
import org.infinispan.server.memcached.interceptors.TextProtocolVisitor;

/**
 * DeleteCommand.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class DeleteCommand implements TextCommand {

   final Cache cache;
   final String key;

   DeleteCommand(Cache cache, String key, long time) {
      this.cache = cache;
      this.key = key;
   }

   @Override
   public CommandType getType() {
      return CommandType.DELETE;
   }

   @Override
   public Object acceptVisitor(ChannelHandlerContext ctx, TextProtocolVisitor next) throws Throwable {
      return next.visitDelete(ctx, this);
   }

   @Override
   public Object perform(ChannelHandlerContext ctx) throws Throwable {
      Channel ch = ctx.getChannel();
      Reply reply;
      Object prev = cache.remove(key);
      reply = reply(prev);
      ChannelBuffers buffers = ctx.getChannelBuffers();
      ch.write(buffers.wrappedBuffer(buffers.wrappedBuffer(reply.bytes()), buffers.wrappedBuffer(CRLF)));
      return reply;
   }

   private Reply reply(Object prev) {
      if (prev == null)
         return Reply.NOT_FOUND;
      else
         return Reply.DELETED;
   }

   public static DeleteCommand newDeleteCommand(Cache cache, String key) {
      return new DeleteCommand(cache, key, 0);
   }
}
