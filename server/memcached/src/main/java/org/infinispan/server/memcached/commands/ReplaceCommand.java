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

import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.server.core.ChannelHandlerContext;
import org.infinispan.server.memcached.Reply;
import org.infinispan.server.memcached.interceptors.TextProtocolVisitor;

/**
 * ReplaceCommand.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class ReplaceCommand extends SetCommand {

   ReplaceCommand(Cache<String, Value> cache, CommandType type, StorageParameters params, byte[] data) {
      super(cache, type, params, data);
   }

   @Override
   public Object acceptVisitor(ChannelHandlerContext ctx, TextProtocolVisitor next) throws Throwable {
      return next.visitReplace(ctx, this);
   }

   @Override
   protected Reply put(String key, int flags, byte[] data, long expiry) {
      Value old = cache.get(key);
      if (old != null) {
         Value value = new Value(flags, data, old.getCas() + 1);
         boolean replaced = cache.replace(params.key, old, value, expiry, TimeUnit.MILLISECONDS);
         return reply(replaced);
      }
      return reply(old);
   }

   private Reply reply(Value prev) {
      if (prev == null)
         return Reply.NOT_STORED;
      else
         return Reply.STORED;
   }

   private Reply reply(boolean replaced) {
      if (!replaced)
         return Reply.NOT_STORED;
      else
         return Reply.STORED;
   }

}
