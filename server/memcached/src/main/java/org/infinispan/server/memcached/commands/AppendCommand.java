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
import org.infinispan.server.core.ChannelHandlerContext;
import org.infinispan.server.memcached.Reply;
import org.infinispan.server.memcached.TextProtocolUtil;
import org.infinispan.server.memcached.interceptors.TextProtocolVisitor;

/**
 * AppendCommand.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class AppendCommand extends SetCommand {

   AppendCommand(Cache<String, Value> cache, CommandType type, StorageParameters params, byte[] data, boolean noReply) {
      super(cache, type, params, data, noReply);
   }

   @Override
   public Object acceptVisitor(ChannelHandlerContext ctx, TextProtocolVisitor next) throws Throwable {
      return next.visitAppend(ctx, this);
   }

   @Override
   protected Reply put(String key, int flags, byte[] data) {
      Value current = cache.get(key);
      if (current != null) {
         byte[] concatenated = concat(current.getData(), data);
         Value next = new Value(current.getFlags(), concatenated, current.getCas() + 1);
         boolean replaced = cache.replace(key, current, next);
         if (replaced)
            return Reply.STORED;
         else
            return Reply.NOT_STORED;
      } else {
         return Reply.NOT_STORED;
      }
   }

   protected byte[] concat(byte[] current, byte[] append) {
      return TextProtocolUtil.concat(current, append);
   }

   @Override
   protected Reply put(String key, int flags, byte[] data, long expiry) {
      return put(key, flags, data); // ignore expiry
   }

}
