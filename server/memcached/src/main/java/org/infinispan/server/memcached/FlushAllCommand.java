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

import static org.infinispan.server.memcached.Reply.OK;
import static org.infinispan.server.memcached.TextProtocolUtil.CRLF;
import static org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.jboss.netty.channel.Channel;

/**
 * FlushAllCommand.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class FlushAllCommand implements Command {
   final Cache cache;
   final long delay;
   final ScheduledExecutorService scheduler;

   FlushAllCommand(Cache cache, long delay, ScheduledExecutorService scheduler) {
      this.cache = cache;
      this.delay = delay;
      this.scheduler = scheduler;
   }

   @Override
   public Object acceptVisitor(Channel ch, CommandInterceptor next) throws Exception {
      return next.visitFlushAll(ch, this);
   }

   @Override
   public CommandType getType() {
      return CommandType.FLUSH_ALL;
   }

   @Override
   public Object perform(Channel ch) throws Exception {
      if (delay == 0) {
         cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL, Flag.SKIP_CACHE_STORE).clear();
      } else {
         scheduler.schedule(new FlushAllDelayed(cache), delay, TimeUnit.SECONDS);
      }
      ch.write(wrappedBuffer(wrappedBuffer(OK.toString().getBytes()), wrappedBuffer(CRLF)));
      return null;
   }

   public static FlushAllCommand newFlushAllCommand(Cache cache, long delay, ScheduledExecutorService scheduler) {
      return new FlushAllCommand(cache, delay, scheduler);
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
