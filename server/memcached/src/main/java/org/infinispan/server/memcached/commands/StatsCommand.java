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

import static org.infinispan.server.memcached.Reply.END;
import static org.infinispan.server.memcached.Reply.STAT;
import static org.infinispan.server.memcached.TextProtocolUtil.CRLF;

import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.server.core.transport.Channel;
import org.infinispan.server.core.transport.ChannelBuffers;
import org.infinispan.server.core.transport.ChannelHandlerContext;
import org.infinispan.server.memcached.InterceptorChain;
import org.infinispan.server.memcached.interceptors.TextProtocolVisitor;

/**
 * StatsCommand.
 * 
 * @author Galder Zamarreño
 * @since 4.1
 */
public class StatsCommand implements TextCommand {
   final Cache cache;
   private final CommandType type;
   final InterceptorChain chain;

   StatsCommand(Cache cache, CommandType type, InterceptorChain chain) {
      this.cache = cache;
      this.type = type;
      this.chain = chain;
   }

   @Override
   public CommandType getType() {
      return CommandType.STATS;
   }

   @Override
   public Object acceptVisitor(ChannelHandlerContext ctx, TextProtocolVisitor next) throws Throwable {
      return next.visitStats(ctx, this);
   }

   @Override
   public Object perform(ChannelHandlerContext ctx) throws Throwable {
      MemcachedStatsImpl stats = new MemcachedStatsImpl(cache.getAdvancedCache().getStats(), chain);
      
      StringBuilder sb = new StringBuilder();
      writeStat("pid", 0, sb, ctx); // Unsupported
      writeStat("uptime", stats.getTimeSinceStart(), sb, ctx);
      writeStat("time", TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()), sb, ctx);
      writeStat("version", cache.getVersion(), sb, ctx);
      writeStat("pointer_size", 0, sb, ctx); // Unsupported
      writeStat("rusage_user", 0, sb, ctx); // Unsupported
      writeStat("rusage_system", 0, sb, ctx); // Unsupported
      writeStat("curr_items", stats.getCurrentNumberOfEntries(), sb, ctx);
      writeStat("total_items", stats.getTotalNumberOfEntries(), sb, ctx);
      writeStat("bytes", 0, sb, ctx); // Unsupported
      writeStat("curr_connections", 0, sb, ctx); // TODO: Through netty?
      writeStat("total_connections", 0, sb, ctx); // TODO: Through netty?
      writeStat("connection_structures", 0, sb, ctx); // Unsupported
      writeStat("cmd_get", stats.getRetrievals(), sb, ctx);
      writeStat("cmd_set", stats.getStores(), sb, ctx);
      writeStat("get_hits", stats.getHits(), sb, ctx);
      writeStat("get_misses", stats.getMisses(), sb, ctx);
      writeStat("delete_misses", stats.getRemoveMisses(), sb, ctx);
      writeStat("delete_hits", stats.getRemoveHits(), sb, ctx);
      writeStat("incr_misses", stats.getIncrMisses(), sb, ctx);
      writeStat("incr_hits", stats.getIncrHits(), sb, ctx);
      writeStat("decr_misses", stats.getDecrMisses(), sb, ctx);
      writeStat("decr_hits", stats.getDecrHits(), sb, ctx);
      writeStat("cas_misses", stats.getCasMisses(), sb, ctx);
      writeStat("cas_hits", stats.getCasHits(), sb, ctx);
      writeStat("cas_badval", stats.getCasBadval(), sb, ctx);
      writeStat("auth_cmds", 0, sb, ctx);  // Unsupported
      writeStat("auth_errors", 0, sb, ctx); // Unsupported
      //TODO: Evictions are measure by evict calls, but not by nodes are that 
      //      are expired after the entry's lifespan has expired.
      writeStat("evictions", stats.getEvictions(), sb, ctx);
      writeStat("bytes_read", 0, sb, ctx); // TODO: Through netty?
      writeStat("bytes_written", 0, sb, ctx); // TODO: Through netty?
      writeStat("limit_maxbytes", 0, sb, ctx); // Unsupported
      writeStat("threads", 0, sb, ctx); // TODO: Through netty?
      writeStat("conn_yields", 0, sb, ctx); // Unsupported

      ChannelBuffers buffers = ctx.getChannelBuffers();
      Channel ch = ctx.getChannel();
      ch.write(buffers.wrappedBuffer(buffers.wrappedBuffer(END.bytes()), buffers.wrappedBuffer(CRLF)));
      
      return END;
   }

   private void writeStat(String stat, Object value, StringBuilder sb, ChannelHandlerContext ctx) {
      ChannelBuffers buffers = ctx.getChannelBuffers();
      Channel ch = ctx.getChannel();
      sb.append(STAT).append(' ').append(stat).append(' ').append(value);
      ch.write(buffers.wrappedBuffer(buffers.wrappedBuffer(sb.toString().getBytes()), buffers.wrappedBuffer(CRLF)));
      sb.setLength(0);
   }

   public static TextCommand newStatsCommand(Cache cache, CommandType type, InterceptorChain chain) {
      return new StatsCommand(cache, type, chain);
   }

}
