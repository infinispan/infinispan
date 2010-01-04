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

import static org.infinispan.server.memcached.Reply.OK;
import static org.infinispan.server.memcached.TextProtocolUtil.CRLF;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.infinispan.server.core.Channel;
import org.infinispan.server.core.ChannelBuffers;
import org.infinispan.server.core.ChannelHandlerContext;
import org.infinispan.server.memcached.interceptors.TextProtocolVisitor;

/**
 * FlushAllCommand.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class FlushAllCommand implements TextCommand {
   final Cache cache;
   final long delay;
   final ScheduledExecutorService scheduler;
   final boolean noReply;

   FlushAllCommand(Cache cache, long delay, ScheduledExecutorService scheduler, boolean noReply) {
      this.cache = cache;
      this.delay = delay;
      this.scheduler = scheduler;
      this.noReply = noReply;
   }

   @Override
   public Object acceptVisitor(ChannelHandlerContext ctx, TextProtocolVisitor next) throws Throwable {
      return next.visitFlushAll(ctx, this);
   }

   @Override
   public CommandType getType() {
      return CommandType.FLUSH_ALL;
   }

   @Override
   public Object perform(ChannelHandlerContext ctx) throws Throwable {
      Channel ch = ctx.getChannel();
      if (delay == 0) {
         cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL, Flag.SKIP_CACHE_STORE).clear();
      } else {
         scheduler.schedule(new FlushAllDelayed(cache), delay, TimeUnit.SECONDS);
      }
      if (!noReply) {
         ChannelBuffers buffers = ctx.getChannelBuffers();
         ch.write(buffers.wrappedBuffer(buffers.wrappedBuffer(OK.bytes()), buffers.wrappedBuffer(CRLF)));
      }
      return OK;
   }

   public static FlushAllCommand newFlushAllCommand(Cache cache, long delay, ScheduledExecutorService scheduler, boolean noReply) {
      return new FlushAllCommand(cache, delay, scheduler, noReply);
   }

   private static class FlushAllDelayed implements Runnable {
      final Cache cache;

      FlushAllDelayed(Cache cache) {
         this.cache = cache;
      }

      @Override
      public void run() {
         cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL, Flag.SKIP_CACHE_STORE).clear();
      }
   }

}
