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
package org.infinispan.server.memcached.interceptors;

import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.server.core.transport.ChannelHandlerContext;
import org.infinispan.server.memcached.Reply;
import org.infinispan.server.memcached.commands.CasCommand;
import org.infinispan.server.memcached.commands.DecrementCommand;
import org.infinispan.server.memcached.commands.IncrementCommand;
import org.infinispan.server.memcached.commands.MemcachedStats;

/**
 * StatsInterceptor.
 * 
 * @author Galder Zamarreño
 * @since 4.1
 */
public class StatsInterceptor extends TextCommandInterceptorImpl implements MemcachedStats {
   private final AtomicLong incrMisses = new AtomicLong(0);
   private final AtomicLong incrHits = new AtomicLong(0);
   private final AtomicLong decrMisses = new AtomicLong(0);
   private final AtomicLong decrHits = new AtomicLong(0);
   private final AtomicLong casMisses = new AtomicLong(0);
   private final AtomicLong casHits = new AtomicLong(0);
   private final AtomicLong casBadval = new AtomicLong(0);

   public StatsInterceptor(TextCommandInterceptor next) {
      super(next);
   }

   @Override
   public Object visitIncrement(ChannelHandlerContext ctx, IncrementCommand command) throws Throwable {
      Object ret = invokeNextInterceptor(ctx, command);
      if (ret != Reply.NOT_FOUND)
         incrHits.incrementAndGet();
      else
         incrMisses.incrementAndGet();
      return ret;
   }

   @Override
   public Object visitDecrement(ChannelHandlerContext ctx, DecrementCommand command) throws Throwable {
      Object ret = invokeNextInterceptor(ctx, command);
      if (ret != Reply.NOT_FOUND)
         decrHits.incrementAndGet();
      else
         decrMisses.incrementAndGet();
      return ret;
   }

   @Override
   public Object visitCas(ChannelHandlerContext ctx, CasCommand command) throws Throwable {
      Reply ret = (Reply) invokeNextInterceptor(ctx, command);
      switch (ret) {
         case STORED:
            casHits.incrementAndGet();
            break;
         case NOT_FOUND:
            casMisses.incrementAndGet();
            break;
         case EXISTS:
            casBadval.incrementAndGet();
            break;
      }
      return ret;
   }

   @Override
   public long getIncrHits() {
      return incrHits.get();
   }

   @Override
   public long getIncrMisses() {
      return incrMisses.get();
   }

   @Override
   public long getDecrHits() {
      return decrHits.get();
   }

   @Override
   public long getDecrMisses() {
      return decrMisses.get();
   }

   @Override
   public long getCasBadval() {
      return casBadval.get();
   }

   @Override
   public long getCasHits() {
      return casHits.get();
   }

   @Override
   public long getCasMisses() {
      return casMisses.get();
   }

}
