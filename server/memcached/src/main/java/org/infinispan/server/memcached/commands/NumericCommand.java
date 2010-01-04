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

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.math.BigInteger;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.server.core.ChannelBuffers;
import org.infinispan.server.core.ChannelHandlerContext;
import org.infinispan.server.core.Channel;
import org.infinispan.server.memcached.Reply;

/**
 * NumericCommand.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public abstract class NumericCommand implements TextCommand {
   private static final Log log = LogFactory.getLog(NumericCommand.class);
   final Cache cache;
   private final CommandType type;
   final String key;
   final BigInteger value;
   final boolean noReply;

   public NumericCommand(Cache cache, CommandType type, String key, BigInteger value, boolean noReply) {
      this.cache = cache;
      this.type = type;
      this.key = key;
      this.value = value;
      this.noReply = noReply;
   }

   public CommandType getType() {
      return type;
   }

   @Override
   public Object perform(ChannelHandlerContext ctx) throws Throwable {
      Channel ch = ctx.getChannel();
      ChannelBuffers buffers = ctx.getChannelBuffers();
      Value old = (Value) cache.get(key);
      if (old != null) {
         BigInteger oldBigInt = old.getData().length == 0 ? BigInteger.valueOf(0) : new BigInteger(old.getData());
         BigInteger newBigInt = operate(oldBigInt, value);
         byte[] newData = newBigInt.toByteArray();
         Value curr = new Value(old.getFlags(), newData, old.getCas() + 1);
         boolean replaced = cache.replace(key, old, curr);
         if (replaced && !noReply) {
            ch.write(buffers.wrappedBuffer(buffers.wrappedBuffer(newBigInt.toString().getBytes()), buffers.wrappedBuffer(CRLF)));
         } else if (!replaced) {
            throw new CacheException("Value modified since we retrieved from the cache, old value was " + oldBigInt);
         }
         return curr;
      } else {
         if (!noReply) {
            ch.write(buffers.wrappedBuffer(buffers.wrappedBuffer(Reply.NOT_FOUND.bytes()), buffers.wrappedBuffer(CRLF)));
         }
         return Reply.NOT_FOUND;
      }
   }

   protected abstract BigInteger operate(BigInteger oldValue, BigInteger newValue);

   public static TextCommand newNumericCommand(Cache cache, CommandType type, String key, BigInteger value, boolean noReply) throws IOException {
      switch(type) {
         case INCR: return new IncrementCommand(cache, type, key, value, noReply);
         case DECR: return new DecrementCommand(cache, type, key, value, noReply);
         default: throw new StreamCorruptedException("Unable to build storage command for type: " + type);
      }
   }
}
